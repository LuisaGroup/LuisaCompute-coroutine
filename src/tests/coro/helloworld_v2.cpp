#include <luisa/runtime/context.h>
#include <luisa/runtime/stream.h>
#include <luisa/runtime/image.h>
#include <luisa/runtime/shader.h>
#include <luisa/dsl/syntax.h>
#include <luisa/dsl/coro/coro_func.h>
#include <stb/stb_image_write.h>
#include <luisa/dsl/sugar.h>

using namespace luisa;
using namespace luisa::compute;

int main(int argc, char *argv[]) {

    Context context{argv[0]};
    if (argc <= 1) { exit(1); }
    Device device = context.create_device(argv[1]);
    Stream stream = device.create_stream();
    constexpr uint2 resolution = make_uint2(1024, 1024);
    Image<float> image{device.create_image<float>(PixelStorage::BYTE4, resolution)};
    luisa::vector<std::byte> host_image(image.view().size_bytes());

    Kernel1D test = [] {
        coro_v2::Generator<uint()> g = [] {
            auto x = def(0u);
            $while (x < 10u) {
                $yield(x);
                x += 1u;
            };
        };
        for (auto x : g()) {
            device_log("x = {}", x);
        }
    };

    auto shader = device.compile(test);
    stream << shader().dispatch(1u) << synchronize();
}
