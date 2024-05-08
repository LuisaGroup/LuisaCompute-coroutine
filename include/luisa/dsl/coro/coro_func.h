//
// Created by Mike on 2024/5/8.
//

#pragma once

#include <luisa/dsl/coro/coro_frame.h>
#include <luisa/dsl/coro/coro_graph.h>
#include <luisa/dsl/func.h>

namespace luisa::compute::inline dsl::coro_v2 {

template<typename T>
class Coroutine {
    static_assert(luisa::always_false_v<T>);
};

template<typename Ret, typename... Args>
class Coroutine<Ret(Args...)> {

public:
    static_assert(std::is_same_v<Ret, void>,
                  "Coroutine function must return void.");

    using Token = CoroGraph::Token;
    static constexpr auto entry_token = CoroGraph::entry_token;

    class Subroutine {

    private:
        Function f;

    private:
        friend class Coroutine;
        explicit Subroutine(Function function) noexcept : f{function} {}

    public:
        void operator()(CoroFrame &frame, detail::prototype_to_callable_invocation_t<Args>... args) const noexcept {
            detail::CallableInvoke invoke;
            invoke << frame.expression();
            static_cast<void>((invoke << ... << args));
            detail::FunctionBuilder::current()->call(f, invoke.args());
        }
    };

private:
    luisa::shared_ptr<const CoroGraph> _graph;

public:
    explicit Coroutine(luisa::shared_ptr<const CoroGraph> graph) noexcept
        : _graph{std::move(graph)} {
        // TODO: check arguments
    }

    template<typename Def>
        requires std::negation_v<is_callable<std::remove_cvref_t<Def>>> &&
                 std::negation_v<is_kernel<std::remove_cvref_t<Def>>>
    Coroutine(Def &&f) noexcept {
        auto coro = detail::FunctionBuilder::define_coroutine([&f] {
            static_assert(std::is_invocable_r_v<void, Def, detail::prototype_to_creation_t<Args>...>);
            auto create = []<size_t... i>(auto &&def, std::index_sequence<i...>) noexcept {
                using arg_tuple = std::tuple<Args...>;
                using var_tuple = std::tuple<Var<std::remove_cvref_t<Args>>...>;
                using tag_tuple = std::tuple<detail::prototype_to_creation_tag_t<Args>...>;
                auto args = detail::create_argument_definitions<var_tuple, tag_tuple>(std::tuple<>{});
                static_assert(std::tuple_size_v<decltype(args)> == sizeof...(Args));
                return luisa::invoke(std::forward<decltype(def)>(def),
                                     static_cast<detail::prototype_to_creation_t<
                                         std::tuple_element_t<i, arg_tuple>> &&>(std::get<i>(args))...);
            };
            create(std::forward<Def>(f), std::index_sequence_for<Args...>{});
            detail::FunctionBuilder::current()->return_(nullptr);// to check if any previous $return called with non-void types
        });
        _graph = CoroGraph::create(coro->function());
    }

public:
    [[nodiscard]] auto graph() const noexcept { return _graph.get(); }
    [[nodiscard]] auto &shared_graph() const noexcept { return _graph; }

public:
    [[nodiscard]] auto instantiate() const noexcept { return _graph->frame()->instantiate(); }
    [[nodiscard]] auto instantiate(Expr<uint3> coro_id) const noexcept { return _graph->frame()->instantiate(coro_id); }
    [[nodiscard]] auto subroutine_count() const noexcept { return _graph->nodes().size(); }
    [[nodiscard]] auto operator[](Token token) const noexcept { return Subroutine{_graph->node(token).cc()}; }
    [[nodiscard]] auto operator[](luisa::string_view name) const noexcept { return Subroutine{_graph->node(name).cc()}; }
    [[nodiscard]] auto entry() const noexcept { return (*this)[entry_token]; }
    [[nodiscard]] auto subroutine(Token token) const noexcept { return (*this)[token]; }
    [[nodiscard]] auto subroutine(luisa::string_view name) const noexcept { return (*this)[name]; }
};

template<typename T>
Coroutine(T &&) -> Coroutine<detail::dsl_function_t<std::remove_cvref_t<T>>>;

}// namespace luisa::compute::inline dsl::coro_v2
