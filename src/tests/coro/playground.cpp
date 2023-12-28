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
    constexpr uint2 resolution = make_uint2(1024, 1024);
    Image<float> image{device.create_image<float>(PixelStorage::BYTE4, resolution)};
    luisa::vector<std::byte> host_image(image.view().size_bytes());

    auto palette = [](Float d) noexcept {
        return lerp(make_float3(0.2f, 0.7f, 0.9f), make_float3(1.0f, 0.0f, 1.0f), d);
    };

    auto rotate = [](Float2 p, Float a) noexcept {
        Var c = cos(a);
        Var s = sin(a);
        return make_float2(dot(p, make_float2(c, s)), dot(p, make_float2(-s, c)));
    };

    auto map = [&rotate](Float3 p, Float time) noexcept {
        for (uint i = 0u; i < 8u; i++) {
            Var t = time * 0.2f;
            p = make_float3(rotate(p.xz(), t), p.y).xzy();
            p = make_float3(rotate(p.xy(), t * 1.89f), p.z);
            p = make_float3(abs(p.x) - 0.5f, p.y, abs(p.z) - 0.5f);
        }
        return dot(copysign(1.0f, p), p) * 0.2f;
    };

    auto rm = [&map, &palette](Float3 ro, Float3 rd, Float time) noexcept {
        Var t = 0.0f;
        Var col = make_float3(0.0f);
        Var d = 0.0f;
        for (UInt i : dynamic_range(64)) {
            Var p = ro + rd * t;
            d = map(p, time) * 0.5f;
            if_(d < 0.02f | d > 100.0f, [] { break_(); });
            col += palette(length(p) * 0.1f) / (400.0f * d);
            t += d;
        }
        return make_float4(col, 1.0f / (d * 100.0f));
    };

    Coroutine coro = [&rm, &rotate, resolution, &image](Var<CoroFrame> &, Float time) noexcept {
        Var xy = coro_id().xy();
        Var res = make_float2(resolution);
        Var uv = (make_float2(xy) - res * 0.5f) / res.x;
        Var ro = make_float3(rotate(make_float2(0.0f, -50.0f), time), 0.0f).xzy();
        Var cf = normalize(-ro);
        Var cs = normalize(cross(cf, make_float3(0.0f, 1.0f, 0.0f)));
        Var cu = normalize(cross(cf, cs));
        Var uuv = ro + cf * 3.0f + uv.x * cs + uv.y * cu;
        Var rd = normalize(uuv - ro);
        Var col = rm(ro, rd, time);
        image->write(xy, make_float4(col.xyz(), 1.0f));
    };

    Kernel2D mega_kernel = [&](Float time) {
        Var<CoroFrame> frame;
        initialize_coroframe(frame, dispatch_id());
        coro(frame, time);
        $loop {
            auto token = read_promise<uint>(frame, "coro_token");
            $switch (token) {
                for (auto i = 1u; i <= coro.suspend_count(); i++) {
                    $case (i) {
                        coro[i](frame, time);
                    };
                }
                $default {
                    $return();
                };
            };
        };
    };

    auto shader = device.compile(mega_kernel);
    stream << shader(1.f).dispatch(resolution)
           << image.copy_to(host_image.data())
           << synchronize();
    stbi_write_png("test_helloworld.png", resolution.x, resolution.y, 4, host_image.data(), 0);
}
