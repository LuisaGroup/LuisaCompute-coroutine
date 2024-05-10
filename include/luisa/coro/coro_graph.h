#pragma once

#include <luisa/core/stl/unordered_map.h>
#include <luisa/coro/coro_node.h>

namespace luisa::compute::inline coro {

class CoroGraph {

private:
    luisa::unordered_map<uint /* token */, CoroNode> _nodes;
    uint _entry;
    const Type *_state_type;
    luisa::unordered_map<luisa::string, uint> _designated_state_members;

public:
    // for construction only
    CoroGraph(uint entry, const Type *state_type) noexcept : _entry{entry}, _state_type{state_type} {}
    [[nodiscard]] CoroNode *add_node(uint token, CoroNode::Func f) noexcept {
        auto node = CoroNode{this, std::move(f)};
        auto [iter, success] = _nodes.emplace(token, std::move(node));
        LUISA_ASSERT(success, "Coroutine node (token = {}) already exists.", token);
        return &(iter->second);
    }
    void designate_state_member(luisa::string name, uint index) noexcept {
        auto [iter, success] = _designated_state_members.emplace(name, index);
        LUISA_ASSERT(success, "State member '{}' already designated.", name);
    }

public:
    [[nodiscard]] const CoroNode *entry() const noexcept {
        return this->node(_entry);
    }
    [[nodiscard]] const CoroNode *node(uint token) const noexcept {
        auto iter = _nodes.find(token);
        LUISA_ASSERT(iter != _nodes.cend(),
                     "Coroutine node (token = {}) not found.", token);
        return &(iter->second);
    }
    [[nodiscard]] auto &nodes() const noexcept { return _nodes; }
    [[nodiscard]] auto state_type() const noexcept { return _state_type; }
    [[nodiscard]] auto &designated_state_members() const noexcept { return _designated_state_members; }
    [[nodiscard]] uint designated_state_member(luisa::string_view name) const noexcept {
        auto iter = _designated_state_members.find(name);
        LUISA_ASSERT(iter != _designated_state_members.cend(),
                     "State member '{}' not designated.", name);
        return iter->second;
    }
};

}// namespace luisa::compute::inline coro
