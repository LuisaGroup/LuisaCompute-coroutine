//
// Created by ChenXin on 2023/9/13.
//

#include <luisa/luisa-compute.h>

using namespace luisa;
using namespace luisa::compute;

struct alignas(4) CoroFrame {
};
LUISA_COROFRAME_STRUCT(CoroFrame){};

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

    constexpr auto n = 2u;
    auto x_buffer = device.create_buffer<uint>(n);
    auto x_vec = std::vector<uint>(n, 0u);

    Coroutine coro = [](Var<CoroFrame> &frame, BufferUInt x_buffer, UInt n) noexcept {
        x_buffer.write(coro_id().x, coro_id().x);
        auto user = def<User>(20u, 1000.0f);
        auto i = def(0u);
        auto x = x_buffer.read(coro_id().x) + user.age;
        $switch (coro_id().x) {
            $case (1u) {
                $if (coro_id().x < 5u) {
                    x_buffer.write(coro_id().x, 10000u + 100u * coro_id().x + coro_token());
                    $if (i == 0u) {
                        x_buffer.write(coro_id().x, 20000u + 100u * coro_id().x + coro_token());
                        $suspend("1");
                    };
                    x = x_buffer.read(coro_id().x);
                    $if (i == 1u) {
                        x_buffer.write(coro_id().x, 30000u + 100u * coro_id().x + coro_token());
                        $suspend("2");
                    };
                    x_buffer.write(coro_id().x, 40000u + 100u * coro_id().x + coro_token());
                    $suspend("3u");
                    x_buffer.write(coro_id().x, x + n);
                }
                $else {
                    x_buffer.write(coro_id().x, x + 2000u);
                };
            };
            $default {
                x_buffer.write(coro_id().x, x + 3000u);
                $suspend("4u");
                x_buffer.write(coro_id().x, x + 4000u);
            };
        };
    };
    LUISA_INFO_WITH_LOCATION("Coro count = {}", coro.suspend_count());
    auto type = Type::of<CoroFrame>();
    auto frame_buffer = device.create_buffer<CoroFrame>(n);
    Kernel1D kernel = [&](BufferUInt x_buffer) noexcept {
        auto id = dispatch_id();
        auto id_x = id.x;
        x_buffer.write(id_x, id_x);
        auto frame = frame_buffer->read(id_x);
        initialize_coroframe(frame, id);
        coro(frame, x_buffer, n);
        frame_buffer->write(id_x, frame);
    };
    auto shader = device.compile(kernel);
    Kernel1D resume_kernel = [&](BufferUInt x_buffer) noexcept {
        auto id = dispatch_x();
        auto frame = frame_buffer->read(id);
        auto token = read_promise<uint>(frame, "token");
        $switch (token) {
            for (int i = 1; i <= coro.suspend_count(); ++i) {
                $case (i) {
                    coro[i](frame, x_buffer, n);
                };
            }
            $default{};
        };
        frame_buffer->write(id, frame);
    };
    auto resume_shader = device.compile(resume_kernel);
    stream << shader(x_buffer).dispatch(n)
           << x_buffer.copy_to(x_vec.data())
           << synchronize();
    for (auto iter = 0u; iter < 5; ++iter) {
        for (auto i = 0u; i < n; ++i) {
            LUISA_INFO("iter {}: x[{}] = {}", iter, i, x_vec[i]);
        }
        stream << resume_shader(x_buffer).dispatch(n)
               << x_buffer.copy_to(x_vec.data())
               << synchronize();
    }
}