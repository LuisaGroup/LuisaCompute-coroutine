//
// Created by Mike on 2024/5/8.
//

#pragma once

#include <luisa/dsl/coro/coro_frame.h>
#include <luisa/dsl/coro/coro_graph.h>
#include <luisa/dsl/func.h>

namespace luisa::compute::inline dsl::coro_v2 {

namespace detail {
LC_DSL_API void coroutine_chained_await_impl(
    CoroFrame &frame, uint node_count,
    luisa::move_only_function<void(CoroGraph::Token, CoroFrame &)> node) noexcept;
LC_DSL_API void coroutine_generator_step_impl(
    CoroFrame &frame, uint node_count, bool is_entry,
    luisa::move_only_function<void(CoroGraph::Token, CoroFrame &)> node) noexcept;
}// namespace detail

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
        void operator()(CoroFrame &frame, compute::detail::prototype_to_callable_invocation_t<Args>... args) const noexcept {
            compute::detail::CallableInvoke invoke;
            invoke << frame.expression();
            static_cast<void>((invoke << ... << args));
            compute::detail::FunctionBuilder::current()->call(f, invoke.args());
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
        auto coro = compute::detail::FunctionBuilder::define_coroutine([&f] {
            static_assert(std::is_invocable_r_v<void, Def, compute::detail::prototype_to_creation_t<Args>...>);
            auto create = []<size_t... i>(auto &&def, std::index_sequence<i...>) noexcept {
                using arg_tuple = std::tuple<Args...>;
                using var_tuple = std::tuple<Var<std::remove_cvref_t<Args>>...>;
                using tag_tuple = std::tuple<compute::detail::prototype_to_creation_tag_t<Args>...>;
                auto args = compute::detail::create_argument_definitions<var_tuple, tag_tuple>(std::tuple<>{});
                static_assert(std::tuple_size_v<decltype(args)> == sizeof...(Args));
                return luisa::invoke(std::forward<decltype(def)>(def),
                                     static_cast<compute::detail::prototype_to_creation_t<
                                         std::tuple_element_t<i, arg_tuple>> &&>(std::get<i>(args))...);
            };
            create(std::forward<Def>(f), std::index_sequence_for<Args...>{});
            compute::detail::FunctionBuilder::current()->return_(nullptr);// to check if any previous $return called with non-void types
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

private:
    template<typename U>
    class Awaiter : public concepts::Noncopyable {
    private:
        U _f;
        luisa::optional<Expr<uint3>> _coro_id;

    private:
        friend class Coroutine;
        explicit Awaiter(U f) noexcept : _f{std::move(f)} {}

    public:
        [[nodiscard]] auto set_id(Expr<uint3> coro_id) && noexcept {
            _coro_id.emplace(coro_id);
            return std::move(*this);
        }
        void await() && noexcept { return _f(std::move(_coro_id)); }
    };

public:
    [[nodiscard]] auto operator()(compute::detail::prototype_to_callable_invocation_t<Args>... args) const noexcept {
        auto f = [=](luisa::optional<Expr<uint3>> coro_id) noexcept {
            auto frame = coro_id ? instantiate(*coro_id) : instantiate();
            detail::coroutine_chained_await_impl(frame, subroutine_count(), [&](Token token, CoroFrame &f) noexcept {
                subroutine(token)(f, args...);
            });
        };
        return Awaiter<decltype(f)>{std::move(f)};
    }
};

template<typename T>
Coroutine(T &&) -> Coroutine<compute::detail::dsl_function_t<std::remove_cvref_t<T>>>;

template<typename T>
class Generator {
    static_assert(luisa::always_false_v<T>);
};

template<typename Ret, typename... Args>
class Generator<Ret(Args...)> {

    static_assert(!std::is_same_v<Ret, void>,
                  "Generator function must not return void.");

private:
    Coroutine<void(Args...)> _coro;

public:
    template<typename Def>
        requires std::negation_v<is_callable<std::remove_cvref_t<Def>>> &&
                 std::negation_v<is_kernel<std::remove_cvref_t<Def>>>
    Generator(Def &&f) noexcept : _coro{std::forward<Def>(f)} {}

public:
    [[nodiscard]] auto coroutine() const noexcept { return _coro; }

private:
    template<typename U>
    class Iterator {
    private:
        luisa::unique_ptr<CoroFrame> _frame;
        U _f;
        bool _invoked{false};
        LoopStmt *_loop{nullptr};

    private:
        friend class Generator;
        Iterator(luisa::unique_ptr<CoroFrame> frame, U f) noexcept
            : _frame{std::move(frame)}, _f{std::move(f)} {}

    public:
        Iterator &operator++() noexcept {
            _invoked = true;
            _f(*_frame, false);
            compute::detail::FunctionBuilder::current()->pop_scope(_loop->body());
            return *this;
        }
        [[nodiscard]] auto operator==(luisa::default_sentinel_t) const noexcept { return _invoked; }
        [[nodiscard]] Var<expr_value_t<Ret>> operator*() noexcept {
            _f(*_frame, true);
            auto fb = compute::detail::FunctionBuilder::current();
            _loop = fb->loop_();
            fb->push_scope(_loop->body());
            dsl::if_(_frame->is_terminated(), [] { dsl::break_(); });
            return _frame->get<Ret>("yield_value");
        }
    };

private:
    template<typename U>
    [[nodiscard]] auto _make_iterator(U u, luisa::optional<Expr<uint3>> coro_id) const noexcept {
        auto frame = luisa::make_unique<CoroFrame>(coro_id ? _coro.instantiate(*coro_id) : _coro.instantiate());
        return Iterator<U>{std::move(frame), std::move(u)};
    }

private:
    template<typename U>
    class Stepper : public concepts::Noncopyable {
    private:
        luisa::optional<Expr<uint3>> _coro_id;
        const Generator &_g;
        U _f;

    private:
        friend class Generator;
        Stepper(const Generator &g, U f) noexcept : _g{g}, _f{std::move(f)} {}

    public:
        [[nodiscard]] auto set_id(Expr<uint3> coro_id) && noexcept {
            _coro_id.emplace(coro_id);
            return std::move(*this);
        }
        [[nodiscard]] auto begin() noexcept { return _g._make_iterator(std::move(_f), std::move(_coro_id)); }
        [[nodiscard]] auto end() const noexcept { return luisa::default_sentinel; }
    };

public:
    [[nodiscard]] auto operator()(compute::detail::prototype_to_callable_invocation_t<Args>... args) const noexcept {
        auto f = [=](CoroFrame &frame, bool is_entry) noexcept {
            detail::coroutine_generator_step_impl(
                frame, _coro.subroutine_count(), is_entry,
                [&](CoroGraph::Token token, CoroFrame &f) noexcept {
                    _coro.subroutine(token)(f, args...);
                });
        };
        return Stepper<decltype(f)>{*this, std::move(f)};
    }
};

}// namespace luisa::compute::inline dsl::coro_v2
