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

    //    Coroutine coro = [](Var<CoroFrame> &frame, BufferUInt x_buffer) noexcept {
    //        auto coro_id_ = coro_id().x;
    //        x_buffer.write(coro_id_, coro_id_);
    //        auto user = def<User>(20u, 1000.0f);
    //        auto x = x_buffer.read(coro_id_) + user.age;
    //        $for (i, 10u) {
    //            $switch (coro_id_ % 2u) {
    //                $case (1u) {
    //                    $if (coro_id_ < 5u) {
    //                        x_buffer.write(coro_id_, 10000u + 100u * coro_id_ + coro_token());
    //                        $if (i == 0u) {
    //                            x_buffer.write(coro_id_, 20000u + 100u * coro_id_ + coro_token());
    //                            $suspend("1");
    //                        };
    ////                        x = x_buffer.read(coro_id_);
    //                        $if (i == 1u) {
    //                            x_buffer.write(coro_id_, 30000u + 100u * coro_id_ + coro_token());
    //                            $suspend("2");
    //                        };
    //                        x_buffer.write(coro_id_, 40000u + 100u * coro_id_ + coro_token());
    //                        $suspend("3u");
    ////                        x_buffer.write(coro_id_, x + n);
    //                    }
    //                    $else {
    //                        x_buffer.write(coro_id_, x + 2000u);
    //                    };
    //                };
    //                $default {
    //                    x_buffer.write(coro_id_, x + 3000u);
    //                    $suspend("4u");
    //                    x_buffer.write(coro_id_, x + 4000u);
    //                };
    //            };
    //            $suspend("5u");
    //        };
    //    };
    Coroutine coro = [](Var<CoroFrame> &frame, BufferUInt x_buffer) noexcept {
        auto coro_id_ = coro_id().x;
        $for (i, 2u) {
            x_buffer.write(coro_id_, 10000u + coro_token());
            $suspend("1");
            //            x_buffer.write(coro_id_, 20000u + coro_token());
            //            $suspend("2");
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
        coro(frame, x_buffer);
        frame_buffer->write(id_x, frame);
    };
    auto shader = device.compile(kernel, {.name = R"(C:\OldNew\Graphics-Lab\LuisaCompute\LuisaCompute-coroutine\output\entry_debug)"});
    Kernel1D resume_kernel = [&](BufferUInt x_buffer) noexcept {
        auto id = dispatch_x();
        auto frame = frame_buffer->read(id);
        auto token = read_promise<uint>(frame, "coro_token");
        $switch (token) {
            for (int i = 1; i <= coro.suspend_count(); ++i) {
                $case (i) {
                    coro[i](frame, x_buffer);
                };
            }
            $default {
                x_buffer.write(id, 0u);
            };
        };
        frame_buffer->write(id, frame);
    };
    auto resume_shader = device.compile(resume_kernel, {.name = R"(C:\OldNew\Graphics-Lab\LuisaCompute\LuisaCompute-coroutine\output\esume_debug)"});
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