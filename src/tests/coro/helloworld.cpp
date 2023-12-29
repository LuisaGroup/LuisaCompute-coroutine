#include <luisa/runtime/context.h>
#include <luisa/runtime/stream.h>
#include <luisa/runtime/image.h>
#include <luisa/runtime/shader.h>
#include <luisa/dsl/syntax.h>
#include <stb/stb_image_write.h>
#include <luisa/dsl/sugar.h>

using namespace luisa;
using namespace luisa::compute;

struct alignas(4) CoroFrame {
};
LUISA_COROFRAME_STRUCT(CoroFrame){};

int main(int argc, char *argv[]) {
    Context context{argv[0]};
    if (argc <= 1) { exit(1); }
    Device device = context.create_device(argv[1]);
    Stream stream = device.create_stream();
    constexpr uint2 resolution = make_uint2(1024, 1024);
    Image<float> image{device.create_image<float>(PixelStorage::BYTE4, resolution)};
    luisa::vector<std::byte> host_image(image.view().size_bytes());

    Coroutine coro = [&](Var<CoroFrame> &frame) noexcept {
        $suspend("1");
        Var coord = coro_id().xy();
        $suspend("2");
        Var uv = (make_float2(coord) + 0.5f) / make_float2(resolution);
        $suspend("3");
        image->write(coord, make_float4(uv, 0.5f, 1.0f));
    };
    auto type = Type::of<CoroFrame>();
    auto frame_buffer = device.create_buffer<CoroFrame>(resolution.x * resolution.y);

    Kernel2D mega_kernel = [&] {
        Var<CoroFrame> frame;
        initialize_coroframe(frame, dispatch_id());
        coro(frame);
        coro[1](frame);
        coro[2](frame);
        coro[3](frame);
    };

    auto shader = device.compile(mega_kernel);
    stream << shader().dispatch(resolution)
           << synchronize();

    stream << image.copy_to(host_image.data())
           << synchronize();
    stbi_write_png("test_helloworld.png", resolution.x, resolution.y, 4, host_image.data(), 0);
}
