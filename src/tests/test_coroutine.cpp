//
// Created by Mike Smith on 2021/2/27.
//

#include "luisa/ast/type_registry.h"
#include "luisa/dsl/soa.h"
#include "luisa/dsl/struct.h"
#include <fstream>
#include <luisa/luisa-compute.h>
#include <luisa/ir/ast2ir.h>
#include <luisa/ir/ir2ast.h>
//#include <luisa/coro/coro_dispatcher.h>
using namespace luisa;
using namespace luisa::compute;
struct alignas(4) CoroFrame {
    //uint id;
    //uint x;
};
struct CoroTest {
    uint id;
    uint x;
};
LUISA_DERIVE_FMT(CoroTest, CoroTest, id, x)
template<>
struct luisa::compute::struct_member_tuple<CoroTest> {
    using this_type = CoroTest;
    using type = std::tuple<LUISA_MAP_LIST(LUISA_STRUCTURE_MAP_MEMBER_TO_TYPE, id, x)>;
    using offset = std::integer_sequence<size_t, LUISA_MAP_LIST(LUISA_STRUCTURE_MAP_MEMBER_TO_OFFSET, id, x)>;
    static_assert(alignof(this_type) >= 4);
    static_assert(luisa::compute::detail::is_valid_reflection_v<this_type, type, offset>);
};
template<>
struct luisa::compute::detail::TypeDesc<CoroTest> {
    using this_type = CoroTest;
    static luisa::string_view description() noexcept {
        static auto s = luisa::compute::detail::make_struct_description(alignof(CoroTest), {LUISA_MAP_LIST(LUISA_STRUCTURE_MAP_MEMBER_TO_DESC, id, x)});
        return s;
    }
};
template<>
struct luisa_compute_extension<CoroTest>;
LUISA_DERIVE_DSL_STRUCT(CoroTest, id, x)
LUISA_DERIVE_SOA(CoroTest, id, x)
template<>
struct luisa_compute_extension<CoroTest> final : luisa::compute::detail::Ref<CoroTest> {};
LUISA_COROFRAME_STRUCT(CoroFrame){};

int main(int argc, char *argv[]) {

    luisa::log_level_verbose();

    auto context = Context{argv[0]};
    if (argc <= 1) {
        LUISA_INFO("Usage: {} <backend>. <backend>: cuda, dx, ispc, metal", argv[0]);
        exit(1);
    }
    auto device = context.create_device(argv[1]);
    constexpr auto n = 7u;
    auto x_buffer = device.create_buffer<float>(n);
    auto stream = device.create_stream(StreamTag::GRAPHICS);

    std::vector<float> x(n);
    std::vector<float> x_out(n);
    for (auto i = 0u; i < n; i++) {
        x[i] = i;
    }
    stream << x_buffer.copy_from(x.data())
           << synchronize();
    static constexpr auto f = [](auto x, auto y) noexcept { return x * sin(y); };
    Coroutine test_coro = [](Var<CoroFrame> &frame, BufferFloat x_buffer, UInt id) noexcept {
        coro_id();
        auto i = id;
        auto x = x_buffer.read(i);
        $suspend(1u, std::make_pair(x, "x"), std::make_pair(x + i, "y"));

        x += 10;
        x_buffer.write(i, x);
        $suspend(2u);
        x += 100;
        x_buffer.write(i, x);
        $suspend(3u);
        $suspend(4u);
    };
    auto test = (Type::of<CoroFrame>())->tag();
    auto frame_buffer = device.create_buffer<CoroFrame>(n);
    auto frame_soa = device.create_soa<CoroFrame>(n);
    //auto a = coro::WavefrontDispatcher{&test_coro, device};
    Kernel1D gen = [&](BufferFloat x_buffer) noexcept {
        auto id = dispatch_x();
        auto frame = frame_soa->read(dispatch_x());
        initialize_coroframe(frame, id);
        test_coro(frame, x_buffer, id);
        if ($read_promise(frame, x) == 0u) {
            frame_buffer->write(dispatch_x(), frame);
        }
    };
    Kernel1D next = [&](BufferFloat x_buffer) noexcept {
        auto id = dispatch_x();
        auto frame = frame_soa->read(dispatch_x());
        test_coro(frame_buffer->read(dispatch_x()), x_buffer, id);
        //if (frame.x != 1) {
        frame_buffer->write(dispatch_x(), frame);
        //}
    };
    ShaderOption option{};
    option.enable_debug_info = true;
    auto gen_shader = device.compile(gen, option);
    auto next_shader = device.compile(next);
    stream << x_buffer.copy_from(x.data())
           << synchronize();
    stream << gen_shader(x_buffer).dispatch(n)
           << synchronize();
    stream << x_buffer.copy_to(x_out.data())
           << synchronize();
    for (int j = 0; j < n; ++j) {
        LUISA_INFO("gen: x[{}] = {}", j, x_out[j]);
    }
    stream << x_buffer.copy_from(x.data())
           << synchronize();
    stream << next_shader(x_buffer).dispatch(n)
           << synchronize();
    stream << x_buffer.copy_to(x_out.data())
           << synchronize();
    for (int j = 0; j < n; ++j) {
        LUISA_INFO("gen: x[{}] = {}", j, x_out[j]);
    }
}
