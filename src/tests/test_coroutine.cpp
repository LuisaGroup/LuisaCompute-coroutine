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
    //uint id;
    //uint x;
};
LUISA_COROFRAME_STRUCT(CoroFrame){};
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
    auto test=(Type::of<CoroFrame>())->tag();
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
        //if (frame.x != 1) {
            frame_buffer->write(dispatch_x(), frame);
        //}
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
