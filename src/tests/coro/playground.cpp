#include <luisa/runtime/context.h>
#include <luisa/runtime/stream.h>
#include <luisa/runtime/image.h>
#include <luisa/runtime/shader.h>
#include <luisa/dsl/syntax.h>
#include <stb/stb_image_write.h>
#include <luisa/dsl/sugar.h>

using namespace luisa;
using namespace luisa::compute;

struct alignas(4) CoroFrame {};
LUISA_COROFRAME_STRUCT(CoroFrame){};

int main(int argc, char *argv[]) {
    Context context{argv[0]};
    if (argc <= 1) { exit(1); }
    Device device = context.create_device(argv[1]);
    Stream stream = device.create_stream();

    auto cuda_print = [](const char *msg) noexcept {
        ExternalCallable<void()>{luisa::format("([] {{ printf(\"{}\n\") }})", msg)}();
    };

    Kernel1D coro = [&]() noexcept {
        Float t;
        // cuda_print("000000000000000000000000");
        $loop {
            // cuda_print("aaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            $if (true) {
                $if (t > 1.f) {
                    $break;
                } $else {
                    t += 1.f;
                    device_log("t = {}", t);
                };
            };
        };
    };

    // Kernel1D mega_kernel = [&] {
    //     Var<CoroFrame> frame;
    //     device_log("scheduler: init");
    //     initialize_coroframe(frame, dispatch_id());
    //     device_log("scheduler: enter entry");
    //     coro(frame);
    //     device_log("scheduler: exit entry");
    //     $loop {
    //         auto token = read_promise<uint>(frame, "coro_token");
    //         $switch (token) {
    //             for (auto i = 1u; i <= coro.suspend_count(); i++) {
    //                 $case (i) {
    //                     device_log("scheduler: enter coro {}", i);
    //                     coro[i](frame);
    //                     device_log("scheduler: exit coro {}", i);
    //                 };
    //             }
    //             $default {
    //                 device_log("scheduler: terminate");
    //                 $return();
    //             };
    //         };
    //     };
    // };

    auto shader = device.compile(coro);
    stream << shader().dispatch(1u)
           << synchronize();
}
