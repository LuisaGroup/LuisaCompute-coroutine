//
// Created by Mike Smith on 2021/2/27.
//

#include "luisa/ast/type_registry.h"
#include "luisa/dsl/struct.h"
#include <fstream>
#include <luisa/luisa-compute.h>
#include <luisa/ir/ast2ir.h>
#include <luisa/ir/ir2ast.h>

using namespace luisa;
using namespace luisa::compute;
struct alignas(4) CoroFrame{
};
struct CoroTest{
    int id;
};
template<>
struct luisa::compute::struct_member_tuple<CoroTest> {
    using this_type = CoroTest;
    using type = std::tuple<LUISA_MAP_LIST(LUISA_STRUCTURE_MAP_MEMBER_TO_TYPE, id)>;
    using offset = std::integer_sequence<size_t, LUISA_MAP_LIST(LUISA_STRUCTURE_MAP_MEMBER_TO_OFFSET, id)>;
    static_assert(alignof(this_type) >= 4);
    static_assert(luisa::compute::detail::is_valid_reflection_v<this_type, type, offset>);
};
template<>
struct luisa::compute::detail::TypeDesc<CoroTest> {
    using this_type = CoroTest;
    static luisa::string_view description() noexcept {
        static auto s = luisa::compute::detail::make_struct_description(alignof(CoroTest), {LUISA_MAP_LIST(LUISA_STRUCTURE_MAP_MEMBER_TO_DESC, id)});
        return s;
    }
};
template<>
struct luisa_compute_extension<CoroTest>;
namespace luisa::compute {
namespace detail {
template<>
class AtomicRef<CoroTest> : private AtomicRefBase {
private:
    using this_type = CoroTest;
    LUISA_MAP(LUISA_STRUCT_MAKE_MEMBER_TYPE, id)
    [[nodiscard]] static constexpr size_t _member_index(std::string_view name) noexcept {
        constexpr const std::string_view member_names[]{LUISA_MAP_LIST(LUISA_STRINGIFY, id)};
        return std::find(std::begin(member_names), std::end(member_names), name) - std::begin(member_names);
    }

public:
    LUISA_MAP(LUISA_STRUCT_MAKE_MEMBER_ATOMIC_REF_DECL, id)
    explicit AtomicRef(const AtomicRefNode *node) noexcept : AtomicRefBase{node} {}
};
}// namespace detail
template<>
struct Expr<CoroTest> {
private:
    using this_type = CoroTest;
    const Expression *_expression;
    LUISA_MAP(LUISA_STRUCT_MAKE_MEMBER_TYPE, id)
    [[nodiscard]] static constexpr size_t _member_index(std::string_view name) noexcept {
        constexpr const std::string_view member_names[]{LUISA_MAP_LIST(LUISA_STRINGIFY, id)};
        return std::find(std::begin(member_names), std::end(member_names), name) - std::begin(member_names);
    }

public:
    LUISA_MAP(LUISA_STRUCT_MAKE_MEMBER_EXPR_DECL, id)
    explicit Expr(const Expression *e) noexcept : _expression{e}, LUISA_MAP_LIST(LUISA_STRUCT_MAKE_MEMBER_INIT, id) {}
    [[nodiscard]] auto expression() const noexcept {
        return this->_expression;
    }
    Expr(Expr &&another) noexcept = default;
    Expr(const Expr &another) noexcept = default;
    Expr &operator=(Expr) noexcept = delete;
    template<size_t i>
    [[nodiscard]] auto get() const noexcept {
        using M = std::tuple_element_t<i, struct_member_tuple_t<CoroTest>>;
        return Expr<M>{detail::FunctionBuilder::current()->member(Type::of<M>(), this->expression(), i)};
    };
};
namespace detail {
template<>
struct Ref<CoroTest> {
private:
    using this_type = CoroTest;
    const Expression *_expression;
    LUISA_MAP(LUISA_STRUCT_MAKE_MEMBER_TYPE, id)
    [[nodiscard]] static constexpr size_t _member_index(std::string_view name) noexcept {
        constexpr const std::string_view member_names[]{LUISA_MAP_LIST(LUISA_STRINGIFY, id)};
        return std::find(std::begin(member_names), std::end(member_names), name) - std::begin(member_names);
    }

public:
    LUISA_MAP(LUISA_STRUCT_MAKE_MEMBER_REF_DECL, id)
    explicit Ref(const Expression *e) noexcept : _expression{e}, LUISA_MAP_LIST(LUISA_STRUCT_MAKE_MEMBER_INIT, id) {}
    [[nodiscard]] auto expression() const noexcept {
        return this->_expression;
    }
    Ref(Ref &&another) noexcept = default;
    Ref(const Ref &another) noexcept = default;
    [[nodiscard]] operator Expr<CoroTest>() const noexcept {
        return Expr<CoroTest>{this->expression()};
    }
    template<typename Rhs>
    void operator=(Rhs &&rhs) &noexcept {
        dsl::assign(*this, std::forward<Rhs>(rhs));
    }
    void operator=(Ref rhs) &noexcept {
        (*this) = Expr{rhs};
    }
    template<size_t i>
    [[nodiscard]] auto get() const noexcept {
        using M = std::tuple_element_t<i, struct_member_tuple_t<CoroTest>>;
        return Ref<M>{detail::FunctionBuilder::current()->member(Type::of<M>(), this->expression(), i)};
    };
    [[nodiscard]] auto operator->() noexcept {
        return reinterpret_cast<luisa_compute_extension<CoroTest> *>(this);
    }
    [[nodiscard]] auto operator->() const noexcept {
        return reinterpret_cast<const luisa_compute_extension<CoroTest> *>(this);
    }
};
}// namespace detail
}// namespace luisa::compute
template<>
struct luisa_compute_extension<CoroTest> final : luisa::compute::detail::Ref<CoroTest> {};
LUISA_CUSTOM_STRUCT_EXT(CoroFrame){};
LUISA_COROFRAME_STRUCT_REFLECT(CoroFrame,"coroframe");
int main(int argc, char *argv[]) {

    luisa::log_level_verbose();

    auto context = Context{argv[0]};
    if (argc <= 1) {
        LUISA_INFO("Usage: {} <backend>. <backend>: cuda, dx, ispc, metal", argv[0]);
        exit(1);
    }
    auto device = context.create_device(argv[1]);
    constexpr auto n = 3u;
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
    Coroutine test_coro = [](Var<CoroFrame> &frame, BufferFloat x_buffer, UInt id
                         ) noexcept {
        auto i = id;
        auto x = x_buffer.read(i);
        $suspend(1u);
        x += 10;
        x_buffer.write(i,x);
        $suspend(2u);
        x += 100;
        x_buffer.write(i,x);
        $suspend(3u);
        x += 1000;
        x_buffer.write(i,x);
        $suspend(4u);
    };
    auto frame_buffer=device.create_buffer<CoroFrame>(n);
    Kernel1D gen = [&](BufferFloat x_buffer) noexcept {
        auto id = dispatch_x();
        auto frame = frame_buffer->read(dispatch_x());
        test_coro(frame, x_buffer,id);
        frame_buffer->write(dispatch_x(), frame);
    };
    Kernel1D next = [&](BufferFloat x_buffer) noexcept {
        auto id = dispatch_x();
        auto frame = frame_buffer->read(dispatch_x());
        test_coro(frame_buffer->read(dispatch_x()),x_buffer,id);
        frame_buffer->write(dispatch_x(), frame);
    };
    auto gen_shader = device.compile(gen);
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
