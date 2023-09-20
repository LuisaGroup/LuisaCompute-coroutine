#pragma once

#include <luisa/core/dll_export.h>
#include <luisa/ast/interface.h>
#include <luisa/coro/coro_transition.h>

namespace luisa::compute::inline coro {

class CoroGraph;

class LC_CORO_API CoroNode : public concepts::Noncopyable {

    friend class CoroGraph;

public:
    using Func = luisa::shared_ptr<const detail::FunctionBuilder>;

private:
    const CoroGraph *_graph;
    Func _function;
    luisa::vector<CoroTransition> _transitions;

protected:
    CoroNode(const CoroGraph *graph, Func function) noexcept;

public:
    // for construction only
    void add_transition(CoroTransition transition) noexcept;

public:
    [[nodiscard]] auto graph() const noexcept { return _graph; }
    [[nodiscard]] auto function() const noexcept { return _function->function(); }
    [[nodiscard]] auto transitions() const noexcept { return luisa::span{_transitions}; }
};

}// namespace luisa::compute::inline coro
