#pragma once

#include <luisa/core/dll_export.h>
#include <luisa/ast/interface.h>
#include <luisa/coro/coro_transition.h>

namespace luisa::compute::inline coro {

class CoroGraph;

class LC_CORO_API CoroNode {

    friend class CoroGraph;

public:
    using Func = luisa::shared_ptr<const detail::FunctionBuilder>;

private:
    const CoroGraph *_graph;
    Func _function;

protected:
    CoroNode(const CoroGraph *graph, Func function) noexcept;

public:
    luisa::vector<uint> input_state_members;
    luisa::vector<uint> output_state_members;

public:
    [[nodiscard]] auto graph() const noexcept { return _graph; }
    [[nodiscard]] auto function() const noexcept { return _function->function(); }
};

}// namespace luisa::compute::inline coro
