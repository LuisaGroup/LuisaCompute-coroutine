#pragma once

#include <luisa/core/dll_export.h>
#include <luisa/core/stl/map.h>
#include <luisa/coro/coro_node.h>

namespace luisa::compute::inline coro {

class LC_CORO_API CoroGraph {

private:
    luisa::unordered_map<uint /* token */, CoroNode> _nodes;
    uint _entry;
    const Type *_state_type;
    luisa::map<uint, uint> _designated_state_members;

public:
    // for construction only
    CoroGraph(uint entry, const Type *state_type) noexcept;
    [[nodiscard]] CoroNode *add_node(uint token, CoroNode::Func f) noexcept;
    void designate_state_member(uint var_id, uint index) noexcept;

public:
    [[nodiscard]] const CoroNode *entry() const noexcept;
    [[nodiscard]] const CoroNode *node(uint token) const noexcept;
    [[nodiscard]] auto &nodes() const noexcept { return _nodes; }
    [[nodiscard]] auto state_type() const noexcept { return _state_type; }
    [[nodiscard]] uint designated_state_member(uint var_id) const noexcept;
};

}// namespace luisa::compute::inline coro
