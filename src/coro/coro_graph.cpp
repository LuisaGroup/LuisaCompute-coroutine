#include <luisa/core/logging.h>
#include <luisa/coro/coro_graph.h>

namespace luisa::compute::inline coro {

const CoroNode *CoroGraph::entry() const noexcept {
    return this->node(_entry);
}

const CoroNode *CoroGraph::node(uint token) const noexcept {
    auto iter = _nodes.find(token);
    LUISA_ASSERT(iter != _nodes.cend(),
                 "Coroutine node (token = {}) not found.", token);
    return &(iter->second);
}

CoroNode *CoroGraph::add_node(uint token, CoroNode::Func f) noexcept {
    auto node = CoroNode{this, std::move(f)};
    auto [iter, success] = _nodes.emplace(token, std::move(node));
    LUISA_ASSERT(success, "Coroutine node (token = {}) already exists.", token);
    return &(iter->second);
}

uint CoroGraph::designated_state_member(uint var_id) const noexcept {
    auto iter = _designated_state_members.find(var_id);
    LUISA_ASSERT(iter != _designated_state_members.cend(),
                 "State member '{}' not designated.", var_id);
    return iter->second;
}

CoroGraph::CoroGraph(uint entry, const Type *state_type) noexcept
    : _entry{entry}, _state_type{state_type} {}

void CoroGraph::designate_state_member(uint var_id, uint index) noexcept {
    auto [iter, success] = _designated_state_members.emplace(var_id, index);
    LUISA_ASSERT(success, "State member '{}' already designated.", var_id);
}

}// namespace luisa::compute::inline coro
