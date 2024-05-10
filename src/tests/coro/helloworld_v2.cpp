#include <luisa/luisa-compute.h>

using namespace luisa;
using namespace luisa::compute;

namespace luisa::compute::coroutine {

}// namespace luisa::compute::coroutine

int main(int argc, char *argv[]) {

    Context context{argv[0]};
    if (argc <= 1) { exit(1); }
    Device device = context.create_device(argv[1]);
    Stream stream = device.create_stream();
    constexpr uint2 resolution = make_uint2(1024, 1024);
    Image<float> image{device.create_image<float>(PixelStorage::BYTE4, resolution)};
    luisa::vector<std::byte> host_image(image.view().size_bytes());

    Kernel1D test = [] {
        coroutine::Generator<uint(uint)> range = [](UInt n) {
            auto x = def(0u);
            $while (x < n) {
                $yield(x);
                x += 1u;
            };
        };
        for (auto x : range(100u)) {
            device_log("x = {}", x);
        }
    };

    coroutine::Coroutine coro = [] {
        $for (i, 10u) {
            device_log("i = {}", i);
            $suspend();
        };
    };

    coroutine::StateMachineCoroScheduler sched{device, coro};
    stream << sched().dispatch(1u, 1u, 1u) << synchronize();
}
