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

    Coroutine coro = [&](Var<CoroFrame> &, Int &a) noexcept {
        $for (i, 10) {
            device_log("before {}: {}", i, a);
            $suspend("a", std::make_pair(a, "x"));
            // device_log("after {}: {}", i, a);
            a += 1;
        };
    };

    Kernel1D mega_kernel = [&] {
        Var<CoroFrame> frame;
        device_log("scheduler: init");
        initialize_coroframe(frame, dispatch_id());
        device_log("scheduler: enter entry");
        auto a = def(0);
        coro(frame, a);
        device_log("scheduler: exit entry");
        $loop {
            auto token = read_promise<uint>(frame, "coro_token");
            $switch (token) {
                for (auto i = 1u; i <= coro.suspend_count(); i++) {
                    $case (i) {
                        device_log("scheduler: enter coro {}, a = {}", i, a);
                        coro[i](frame, a);
                        device_log("promise x: {}", read_promise<int>(frame, "x"));
                        device_log("scheduler: exit coro {}, a = {}", i, a);
                    };
                }
                $default {
                    device_log("scheduler: terminate");
                    $return();
                };
            };
        };
    };

    auto shader = device.compile(mega_kernel);
    stream << shader().dispatch(1u)
           << synchronize();
}
