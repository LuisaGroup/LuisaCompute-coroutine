//
// Created by ChenXin on 2023/9/13.
//

#include <luisa/luisa-compute.h>
#include<luisa/coro/coro_dispatcher.h>
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
    constexpr auto n = 40u;
    auto x_buffer = device.create_buffer<uint>(n);
    auto x_vec = std::vector<uint>(n, 0u);

    Coroutine coro = [](Var<CoroFrame> &frame, BufferUInt x_buffer, UInt n) noexcept {
        auto id = coro_id().x;
        //        x_buffer.write(id, id * 2u);
        x_buffer.write(id, coro_token() +1000u + 10 * id);
        $suspend("1");
        x_buffer.write(id, coro_token() +2000u + 20 * id);
        //        $if(id % 2u == 0u) {
        //            $suspend("1");
        //            x_buffer.write(id, 1000u + coro_token() * 2u);
        //        };
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
    auto frame_buffer = device.create_soa<CoroFrame>(n);
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
        auto token = read_promise<uint>(frame, "coro_token");
        $switch (token) {
            for (int i = 1; i <= coro.suspend_count(); ++i) {
                $case (i) {
                    coro[i](frame, x_buffer, n);
                };
            }
        };
        frame_buffer->write(id, frame);
    };
    Kernel1D clear = [&](BufferUInt x_buffer) noexcept {
        auto id = dispatch_x();
        x_buffer.write(id, 0u);
    };
    auto clear_shader = device.compile(clear);
    coro::SimpleCoroDispatcher Sdispatcher{&coro, device, n};
    stream << clear_shader(x_buffer).dispatch(n);
    Sdispatcher(x_buffer, n, n);
    LUISA_INFO("Simple Dispatcher:");
    stream << Sdispatcher.await_all()
           << x_buffer.copy_to(x_vec.data())
           << synchronize();
    for (auto i = 0u; i < n; ++i) {
        LUISA_INFO("x[{}] = {}", i, x_vec[i]);
    }
    coro::PersistentCoroDispatcher PTdispatcher{&coro, device, stream, 64u, 32u, 1u, false};
    stream << clear_shader(x_buffer).dispatch(n);
    PTdispatcher(x_buffer, n, n);
    /*for (auto iter = 0u; iter < 6; ++iter) {
        stream << dispatcher.await_step()
               << x_buffer.copy_to(x_vec.data())
               << synchronize();
        for (auto i = 0u; i < n; ++i) {
            LUISA_INFO("iter {}: x[{}] = {}", iter, i, x_vec[i]);
        }
    }*/
    stream << PTdispatcher.await_all()
           << x_buffer.copy_to(x_vec.data())
           << synchronize();
    LUISA_INFO("Persistent Thread Dispatcher:");
    for (auto i = 0u; i < n; ++i) {
        LUISA_INFO("x[{}] = {}", i, x_vec[i]);
    }
    coro::WavefrontCoroDispatcher Wdispatcher{&coro, device, stream, 20, {}, false};
    stream << clear_shader(x_buffer).dispatch(n);

    Wdispatcher(x_buffer, n, n);
    LUISA_INFO("Wavefront Dispatcher:");
    stream << Wdispatcher.await_all()
           << x_buffer.copy_to(x_vec.data())
           << synchronize();
    for (auto i = 0u; i < n; ++i) {
        LUISA_INFO("x[{}] = {}", i, x_vec[i]);
    }
}