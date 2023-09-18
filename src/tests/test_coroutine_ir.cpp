//
// Created by ChenXin on 2023/9/13.
//

#include <luisa/luisa-compute.h>

using namespace luisa;
using namespace luisa::compute;

struct alignas(4) CoroFrame{
};
LUISA_CUSTOM_STRUCT_EXT(CoroFrame){};
LUISA_COROFRAME_STRUCT_REFLECT(CoroFrame,"coroframe");

int main(int argc, char *argv[]) {
    log_level_verbose();

    auto context = Context{argv[0]};
    if (argc <= 1) {
        LUISA_INFO("Usage: {} <backend>. <backend>: cuda, dx, ispc, metal", argv[0]);
        exit(1);
    }
    auto device = context.create_device(argv[1]);
    auto stream = device.create_stream(StreamTag::COMPUTE);

    constexpr auto n = 10u;
    auto x_buffer = device.create_buffer<uint>(n);
    auto x_vec = std::vector<uint>(n, 0u);

    Coroutine coro = [](Var<CoroFrame> &frame, BufferUInt x_buffer, UInt id, UInt n) noexcept {
        auto x = x_buffer.read(id);
        $loop {
            $if(id < 5u) {
                x_buffer.write(id, x + 10u);
                $suspend(1u);
                x = x_buffer.read(id);
                x_buffer.write(id, x + 100u);
            } $else {
                x_buffer.write(id, x + 1000u);
            };
        };
        x_buffer.write(id, x + 10000u);
    };
    auto frame_buffer=device.create_buffer<CoroFrame>(n);
    Kernel1D kernel = [&](BufferUInt x_buffer) noexcept {
        auto id = dispatch_x();
        x_buffer.write(id, id);
        auto frame = frame_buffer->read(id);
        coro(frame, x_buffer, id, n);
    };
    auto shader = device.compile(kernel);

    stream << shader(x_buffer).dispatch(n)
           << x_buffer.copy_to(x_vec.data())
           << synchronize();
    for (auto i = 0u; i < n; ++i) {
        LUISA_INFO("x[{}] = {}", i, x_vec[i]);
    }
}