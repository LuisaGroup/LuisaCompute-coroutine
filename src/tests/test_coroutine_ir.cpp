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

    constexpr auto n = 10u;
    auto x_buffer = device.create_buffer<uint>(n);
    auto x_vec = std::vector<uint>(n, 0u);

    Coroutine coro = [](Var<CoroFrame> &frame, BufferUInt x_buffer, UInt n) noexcept {
        auto user = def<User>(20u, 1000.0f);
        auto i = def(0u);
        auto id = coro_id().x;
        $while (i <= 3u) {
            auto x = x_buffer.read(id) + user.age;
            $switch (1u) {
                $case (1u) {
                    $if (id < 5u) {
                        x_buffer.write(id, x + 1000u);
                        $if (i == 0u) {
                            $suspend("1");
                        };
                        x = x_buffer.read(id);
                        $if (i == 1u) {
                            $suspend("2");
                        };
                        x = x;
                        $suspend("3u");// TODO: without if, unwrap bug may occur
                        x_buffer.write(id, x + n);
                    }
                    $else {
                        x_buffer.write(id, x + 2000u);
                    };
                };
                $default {
                    x_buffer.write(id, x + 3000u);
                };
            };
            i += 1u;
        };
    };
    auto frame_buffer = device.create_buffer<CoroFrame>(n);
    Kernel1D kernel = [&](BufferUInt x_buffer) noexcept {
        auto id = dispatch_x();
        x_buffer.write(id, id);
        auto frame = frame_buffer->read(id);
        coro(frame, x_buffer, n);
        frame_buffer->write(id, frame);
    };
    auto shader = device.compile(kernel);
    Kernel1D resume_kernel = [&](BufferUInt x_buffer) noexcept {
        auto id = dispatch_x();
        auto frame = frame_buffer->read(id);
        auto token = read_promise<uint>(frame,"frame_token");
        $switch(token){
          for(int i=1;i<=coro.suspend_count();++i){
              $case(i){
                coro[i](frame,x_buffer,n);
              };
          }
          $default{
          };
        };
        frame_buffer->write(id, frame);
    };
    auto resume_shader = device.compile(resume_kernel);
    stream << shader(x_buffer).dispatch(n)
           << x_buffer.copy_to(x_vec.data())
           << synchronize();
    for(auto iter=0u; iter<20;++iter) {
        for (auto i = 0u; i < n; ++i) {
            LUISA_INFO("iter {}: x[{}] = {}", iter, i, x_vec[i]);
        }
        stream<<resume_shader(x_buffer).dispatch(n)
        << x_buffer.copy_to(x_vec.data())
        <<synchronize();
    }
}