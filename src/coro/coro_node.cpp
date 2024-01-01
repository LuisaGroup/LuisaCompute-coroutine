#include <luisa/coro/coro_node.h>

namespace luisa::compute::inline coro {

CoroNode::CoroNode(const CoroGraph *graph,
                   CoroNode::Func function) noexcept
    : _graph{graph}, _function{std::move(function)} {}
}// namespace luisa::compute::inline coro
