//
// Created by Mike on 2024/5/8.
//

#pragma once

#include <luisa/core/dll_export.h>
#include <luisa/coro/coro_frame.h>
#include <luisa/coro/coro_graph.h>
#include <luisa/dsl/func.h>

namespace luisa::compute::coroutine {

namespace detail {
LC_CORO_API void coroutine_chained_await_impl(
    CoroFrame &frame, uint node_count,
    luisa::move_only_function<void(CoroToken, CoroFrame &)> node) noexcept;
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
    [[nodiscard]] auto frame() const noexcept { return _graph->frame(); }
    [[nodiscard]] auto &shared_frame() const noexcept { return _graph->shared_frame(); }

public:
    [[nodiscard]] auto instantiate() const noexcept { return CoroFrame::create(_graph->shared_frame()); }
    [[nodiscard]] auto instantiate(Expr<uint3> coro_id) const noexcept { return CoroFrame::create(_graph->shared_frame(), coro_id); }
    [[nodiscard]] auto subroutine_count() const noexcept { return static_cast<uint>(_graph->nodes().size()); }
    [[nodiscard]] auto operator[](CoroToken token) const noexcept { return Subroutine{_graph->node(token).cc()}; }
    [[nodiscard]] auto operator[](luisa::string_view name) const noexcept { return Subroutine{_graph->node(name).cc()}; }
    [[nodiscard]] auto entry() const noexcept { return (*this)[coro_token_entry]; }
    [[nodiscard]] auto subroutine(CoroToken token) const noexcept { return (*this)[token]; }
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
        auto f = [=, this](luisa::optional<Expr<uint3>> coro_id) noexcept {
            auto frame = coro_id ? instantiate(*coro_id) : instantiate();
            detail::coroutine_chained_await_impl(frame, subroutine_count(), [&](CoroToken token, CoroFrame &f) noexcept {
                subroutine(token)(f, args...);
            });
        };
        return Awaiter<decltype(f)>{std::move(f)};
    }
};

namespace detail {
struct CoroAwaitInvoker {
    template<typename A>
    void operator%(A &&awaiter) && noexcept {
        std::forward<A>(awaiter).await();
    }
};
}// namespace detail

template<typename T>
Coroutine(T &&) -> Coroutine<compute::detail::dsl_function_t<std::remove_cvref_t<T>>>;

template<typename T>
class Generator {
    static_assert(luisa::always_false_v<T>);
};

namespace detail {

LC_CORO_API void coroutine_generator_next_impl(
    CoroFrame &frame, uint node_count,
    const luisa::move_only_function<void(CoroFrame &, CoroToken)> &resume) noexcept;

template<typename T>
class GeneratorIter : public concepts::Noncopyable {

private:
    uint _n;
    CoroFrame _frame;
    using Resume = luisa::move_only_function<void(CoroFrame &, CoroToken)>;
    Resume resume;

private:
    template<typename U>
    friend class Generator;
    GeneratorIter(uint n, CoroFrame frame, Resume resume) noexcept
        : _n{n}, _frame{std::move(frame)}, resume{std::move(resume)} {}

public:
    [[nodiscard]] auto set_id(Expr<uint3> coro_id) && noexcept {
        _frame.coro_id = coro_id;
        return std::move(*this);
    }
    [[nodiscard]] Bool has_next() const noexcept { return !_frame.is_terminated(); }
    [[nodiscard]] Var<T> next() noexcept {
        coroutine_generator_next_impl(_frame, _n, resume);
        return _frame.get<T>("__yielded_value");
    }

private:
    class RangeForIterator {
    private:
        GeneratorIter &_g;
        bool _invoked{false};
        LoopStmt *_loop{nullptr};

    private:
        friend class GeneratorIter;
        explicit RangeForIterator(GeneratorIter &g) noexcept : _g{g} {}

    public:
        RangeForIterator &operator++() noexcept {
            _invoked = true;
            compute::detail::FunctionBuilder::current()->pop_scope(_loop->body());
            return *this;
        }
        [[nodiscard]] bool operator==(luisa::default_sentinel_t) const noexcept { return _invoked; }
        [[nodiscard]] Var<T> operator*() noexcept {
            auto fb = compute::detail::FunctionBuilder::current();
            _loop = fb->loop_();
            fb->push_scope(_loop->body());
            dsl::if_(!_g.has_next(), [] { dsl::break_(); });
            return _g.next();
        }
    };

public:
    [[nodiscard]] auto begin() noexcept { return RangeForIterator{*this}; }
    [[nodiscard]] auto end() const noexcept { return luisa::default_sentinel; }
};

}// namespace detail

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

public:
    [[nodiscard]] auto operator()(compute::detail::prototype_to_callable_invocation_t<Args>... args) const noexcept {
        return detail::GeneratorIter<Ret>{
            _coro.subroutine_count(),
            _coro.instantiate(),
            [=, this](CoroFrame &frame, CoroToken token) noexcept {
                _coro[token](frame, args...);
            },
        };
    }
};

}// namespace luisa::compute::coroutine
