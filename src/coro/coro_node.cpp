#include <luisa/coro/coro_node.h>

namespace luisa::compute::inline coro {

CoroNode::CoroNode(const CoroGraph *graph,
                   CoroNode::Func function) noexcept
    : _graph{graph}, _function{std::move(function)} {}

void CoroNode::add_transition(CoroTransition transition) noexcept {
#ifndef NDEBUG
    auto already_added = std::find_if(
        _transitions.cbegin(), _transitions.cend(),
        [&](const auto &t) noexcept {
            return t.destination == transition.destination;
        });
    LUISA_ASSERT(already_added == _transitions.cend(),
                 "Transition to node {} already added.",
                 transition.destination);
#endif
    _transitions.emplace_back(std::move(transition));
}

}// namespace luisa::compute::inline coro
