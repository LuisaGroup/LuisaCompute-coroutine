//
// Created by ChenXin on 2023/9/13.
//

#include <luisa/luisa-compute.h>
#include <luisa/coro/coro_dispatcher.h>

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

    constexpr auto n = 3u;
    auto x_buffer = device.create_buffer<uint>(n);
    auto x_vec = std::vector<uint>(n, 0u);

    //    Coroutine coro = [](Var<CoroFrame> &frame, BufferUInt x_buffer) noexcept {
    //        auto coro_id_ = coro_id().x;
    //        x_buffer.write(coro_id_, coro_id_);
    //        auto user = def<User>(20u, 1000.0f);
    //        auto x = x_buffer.read(coro_id_) + user.age;
    //        $for (i, 3u) {
    //            $switch (coro_id_ % 2u) {
    //                $case (1u) {
    //                    $if (coro_id_ < 5u) {
    //                        x_buffer.write(coro_id_, 10000u + 100u * coro_id_ + coro_token());
    //                        $if (i == 0u) {
    //                            x_buffer.write(coro_id_, 20000u + 100u * coro_id_ + coro_token());
    //                            $suspend("1");
    //                        };
    //                        //                        x = x_buffer.read(coro_id_);
    //                        $if (i == 1u) {
    //                            x_buffer.write(coro_id_, 30000u + 100u * coro_id_ + coro_token());
    //                            $suspend("2");
    //                        };
    //                        x_buffer.write(coro_id_, 40000u + 100u * coro_id_ + coro_token());
    //                        $suspend("3u");
    //                        //                        x_buffer.write(coro_id_, x + n);
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
        auto a = def(0u);
        $for (i, 2u) {
            $suspend("Suspend(1)");
            x_buffer.write(coro_id_, coro_id_);

            a = def(234u);
            $suspend("2");

            $if (1u == 0u) {
                a = def(7u);
            };

            $suspend("Suspend(2)");

            x_buffer.write(coro_id_, a);
        };

        //        auto coro_id_ = coro_id().x;
        //        UInt a;
        //        $for (i, 2u) {
        //            a = def(0u);
        //            $suspend("1");
        //
        //            $suspend("2");
        //            x_buffer.write(coro_id_, a);
        //        };
    };
    //    now:
    //    $for (i, 2u) {
    //        auto a = def(1u);
    //        $suspend("1");
    //
    //        auto flag = def(false);
    //
    //        $if (i == 0u) {
    //            flag = def(false);
    //        };
    //        $if (flag) {
    //            $suspend("2");
    //
    //            x_buffer.write(coro_id_, a);
    //        };
    //
    //        auto a = def(1u);
    //        $suspend("1");
    //    };
    LUISA_INFO_WITH_LOCATION("Coro count = {}", coro.suspend_count());
    auto type = Type::of<CoroFrame>();
    Kernel1D mega_kernel = [&](BufferUInt x_buffer) noexcept {
        auto id = dispatch_x();
        Var<CoroFrame> frame;
        initialize_coroframe(frame, dispatch_id());
        coro(frame, x_buffer);
        $loop {
            auto token = read_promise<uint>(frame, "coro_token");
            device_log("id = {}, coro_token = {}, coro_frame = {}", id, token, frame);
            $switch (token) {
                for (int i = 1; i <= coro.suspend_count(); ++i) {
                    $case (i) {
                        coro[i](frame, x_buffer);
                    };
                }
                $default {
                    x_buffer.write(id, 0u);
                    $return();
                };
            };
        };
    };
    auto shader = device.compile(mega_kernel, {.name = R"(output\shader)"});
    // coro::WavefrontCoroDispatcher dispatcher{&coro, device, stream, n, false};
    // dispatcher(x_buffer, n);
    // stream << dispatcher.await_all()
    //        << synchronize()
    //        << x_buffer.copy_to(x_vec.data());
    for (auto i = 0u; i < n; ++i) {
        LUISA_INFO("x[{}] = {}", i, x_vec[i]);
    }
    stream << shader(x_buffer).dispatch(n);
    stream << synchronize();
}