//
// Created by ChenXin on 2023/9/13.
//

#include <luisa/luisa-compute.h>

using namespace luisa;
using namespace luisa::compute;

struct alignas(4) CoroFrame {
};
LUISA_CUSTOM_STRUCT_EXT(CoroFrame){};
LUISA_COROFRAME_STRUCT_REFLECT(CoroFrame, "CoroFrame")

struct alignas(8) User {
public:
    uint age;
    float savings;
};
LUISA_STRUCT(User, age, savings) {};

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

    Coroutine coro = [](Var<CoroFrame> &frame, BufferUInt x_buffer, UInt n) noexcept {
        auto id = coro_id().x;
        x_buffer.write(id, id * 2u);
//        x_buffer.write(id, coro_token() * 2u);
        $suspend("1");
//        auto user = def<User>(20u, 1000.0f);
//        auto i = def(0u);
//        $while (i <= 3u) {
//            auto x = x_buffer.read(id) + user.age;
//            $switch (1u) {
//                $case (1u) {
//                    $if (id < 5u) {
//                        x_buffer.write(id, x + 1000u);
//                        $if (i == 0u) {
//                            $suspend("1");
//                        };
//                        x = x_buffer.read(id);
//                        $if (i == 1u) {
//                            $suspend("2");
//                        };
//                        x = x;
//                        $suspend("3u");
//                        x_buffer.write(id, x + n);
//                    }
//                    $else {
//                        x_buffer.write(id, x + 2000u);
//                    };
//                };
//                $default {
//                    x_buffer.write(id, x + 3000u);
//                };
//            };
//            i += 1u;
//        };
    };
    auto type = Type::of<CoroFrame>();
    auto frame_buffer = device.create_buffer<CoroFrame>(n);
    Kernel1D kernel = [&](BufferUInt x_buffer) noexcept {
        auto id = dispatch_id();
        auto id_x = id.x;
        x_buffer.write(id_x, id_x);
        auto frame = frame_buffer->read(id_x);
        initialize_coroframe(frame, id);
        coro(frame, x_buffer, n);
    };
    auto shader = device.compile(kernel);

    stream << shader(x_buffer).dispatch(n)
           << x_buffer.copy_to(x_vec.data())
           << synchronize();
    for (auto i = 0u; i < n; ++i) {
        LUISA_INFO("x[{}] = {}", i, x_vec[i]);
    }
}