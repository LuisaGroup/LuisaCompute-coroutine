//
// Created by Mike on 2024/5/10.
//

#pragma once

#include <luisa/coro/v2/coro_func.h>
#include <luisa/coro/v2/coro_graph.h>
#include <luisa/coro/v2/coro_scheduler.h>

namespace luisa::compute::coroutine {

struct WavefrontCoroSchedulerConfig {
    uint3 block_size = luisa::make_uint3(128, 1, 1);
    uint max_instance_count = 2_M;
    bool soa = true;
    bool sort = true;// use sort for coro token gathering
    bool compact = true;
    bool debug = false;
    uint hint_range = 0xffff'ffff;
    luisa::vector<luisa::string> hint_fields;
};

template<typename... Args>
class WavefrontCoroScheduler : public CoroScheduler<Args...> {

private:
    Shader3D<Args...> _shader;

private:
    void _create_shader(Device &device, const Coroutine<void(Args...)> &coro,
                        const WavefrontCoroSchedulerConfig &config) noexcept {
    }

    void _dispatch(Stream &stream, uint3 dispatch_size,
                   compute::detail::prototype_to_shader_invocation_t<Args>... args) noexcept override {
        LUISA_ERROR_WITH_LOCATION("Unimplemented");
    }

public:
    WavefrontCoroScheduler(Device &device, const Coroutine<void(Args...)> &coro,
                           const WavefrontCoroSchedulerConfig &config) noexcept {
        _create_shader(device, coro, config);
    }
    WavefrontCoroScheduler(Device &device, const Coroutine<void(Args...)> &coro) noexcept
        : WavefrontCoroScheduler{device, coro, WavefrontCoroSchedulerConfig{}} {}
};

template<typename... Args>
WavefrontCoroScheduler(Device &, const Coroutine<void(Args...)> &)
    -> WavefrontCoroScheduler<Args...>;

template<typename... Args>
WavefrontCoroScheduler(Device &, const Coroutine<void(Args...)> &,
                       const WavefrontCoroSchedulerConfig &)
    -> WavefrontCoroScheduler<Args...>;

}// namespace luisa::compute::coroutine