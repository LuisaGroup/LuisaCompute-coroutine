//
// Created by Mike on 2024/5/8.
//

#include <luisa/dsl/sugar.h>
#include <luisa/dsl/coro/coro_func.h>

namespace luisa::compute::inline dsl::coro_v2::detail {

void coroutine_chained_await_impl(CoroFrame &frame, uint node_count,
                                  luisa::move_only_function<void(CoroGraph::Token, CoroFrame &)> node) noexcept {
    node(CoroGraph::entry_token, frame);
    $while (frame.target_token != CoroGraph::terminal_token) {
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

}// namespace luisa::compute::inline dsl::coro_v2::detail
