//
// Created by Mike on 2024/5/10.
//

#pragma once

#include <luisa/coro/v2/coro_func.h>
#include <luisa/coro/v2/coro_scheduler.h>

namespace luisa::compute::coroutine {

namespace detail {
LC_CORO_API void coro_scheduler_state_machine_impl(
    CoroFrame &frame, uint state_count,
    luisa::move_only_function<void(CoroToken)> node) noexcept;
}// namespace detail

template<typename... Args>
class StateMachineCoroScheduler : public CoroScheduler<Args...> {

private:
    Shader3D<Args...> _shader;

private:
    [[nodiscard]] static auto _create_shader(Device &device, const Coroutine<void(Args...)> &coro) noexcept {
        Kernel3D kernel = [&coro](Var<Args>... args) noexcept {
            set_block_size(128u, 1u, 1u);
            auto frame = coro.instantiate(dispatch_id());
            detail::coro_scheduler_state_machine_impl(
                frame, coro.subroutine_count(),
                [&](CoroToken token) noexcept {
                    coro.subroutine(token)(frame, args...);
                });
        };
        return device.compile(kernel);
    }

    void _dispatch(Stream &stream, uint3 dispatch_size,
                   compute::detail::prototype_to_shader_invocation_t<Args>... args) noexcept override {
        stream << _shader(args...).dispatch(dispatch_size);
    }

public:
    StateMachineCoroScheduler(Device &device, const Coroutine<void(Args...)> &coro) noexcept
        : _shader{_create_shader(device, coro)} {}
};

template<typename... Args>
StateMachineCoroScheduler(Device &, const Coroutine<void(Args...)> &) -> StateMachineCoroScheduler<Args...>;

}// namespace luisa::compute::coroutine
