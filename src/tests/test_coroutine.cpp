//
// Created by Mike Smith on 2021/2/27.
//

#include <fstream>
#include <luisa/luisa-compute.h>
#include <luisa/ir/ast2ir.h>
#include <luisa/ir/ir2ast.h>

using namespace luisa;
using namespace luisa::compute;

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
        x[i] = 0;
    }
    stream << x_buffer.copy_from(x.data())
           << synchronize();

    static constexpr auto f = [](auto x, auto y) noexcept { return x * sin(y); };

    Kernel1D kernel = [](BufferFloat x_buffer, 
                         UInt handle) noexcept {
        //just for practice: write a program run to suspend corresponding handle, no continue
        auto i = dispatch_x();
        auto x = x_buffer.read(i);
        x += 1;
        $suspend(handle);
        x += 10;
        x_buffer.write(i,x);
        $suspend(handle);
        x += 100;
        x_buffer.write(i,x);
        $suspend(handle);
        x += 1000;
        x_buffer.write(i,x);
        $suspend(handle);
    };

    auto kernel_shader = device.compile(kernel);
    for (int i = 0; i < 10; ++i) {
        stream << kernel_shader(x_buffer, i).dispatch(n)
           << synchronize();
        stream << x_buffer.copy_to(x_out.data())
               << synchronize();
        for (int j = 0; j < n; ++j) {
            LUISA_INFO("x[{}] = {}", j, x_out[j]);
        }
    }
}
