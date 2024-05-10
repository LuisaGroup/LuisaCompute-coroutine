//
// Created by Mike on 2024/5/10.
//

#include <luisa/dsl/sugar.h>
#include <luisa/coro/v2/schedulers/state_machine.h>

namespace luisa::compute::coroutine::detail {
inline void coro_scheduler_state_machine_impl(CoroFrame &frame, uint state_count,
                                              luisa::move_only_function<void(CoroToken)> node) noexcept {
    node(coro_token_entry);
    $loop {
        $switch (frame.target_token) {
            for (auto i = 1u; i < state_count; i++) {
                $case (i) { node(i); };
            }
            $default { $return(); };
        };
    };
}
}// namespace luisa::compute::coroutine::detail
