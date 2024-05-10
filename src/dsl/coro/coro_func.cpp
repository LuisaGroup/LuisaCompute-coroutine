//
// Created by Mike on 2024/5/8.
//

#include <luisa/dsl/sugar.h>
#include <luisa/dsl/coro/coro_func.h>

namespace luisa::compute::coroutine::detail {

void coroutine_chained_await_impl(CoroFrame &frame, uint node_count,
                                  luisa::move_only_function<void(CoroToken, CoroFrame &)> node) noexcept {
    node(coro_token_entry, frame);
    $while (!frame.is_terminated()) {
        $suspend();
        $switch (frame.target_token) {
            for (auto i = 1u; i < node_count; i++) {
                $case (i) {
                    node(i, frame);
                };
            }
            $default { dsl::unreachable(); };
        };
    };
}

void coroutine_generator_step_impl(CoroFrame &frame, uint node_count, bool is_entry,
                                   luisa::move_only_function<void(CoroToken, CoroFrame &)> node) noexcept {
    if (is_entry) {
        node(coro_token_entry, frame);
    } else {
        $switch (frame.target_token) {
            for (auto i = 1u; i < node_count; i++) {
                $case (i) { node(i, frame); };
            }
            $default { dsl::unreachable(); };
        };
    }
}

}// namespace luisa::compute::coro_v2::detail
