//
// Created by Mike on 2024/5/10.
//

#pragma once

#include <luisa/dsl/shared.h>
#include <luisa/coro/coro_frame.h>

namespace luisa::compute {

template<>
class Shared<coroutine::CoroFrame> {

private:
    luisa::shared_ptr<const coroutine::CoroFrameDesc> _desc;
    luisa::vector<const RefExpr *> _expressions;
    size_t _size;

private:
    void _create(bool soa, luisa::span<const uint> soa_excluded) noexcept {
        auto fb = detail::FunctionBuilder::current();
        if (!soa) {
            auto s = fb->shared(Type::array(_desc->type(), _size));
            _expressions.emplace_back(s);
        } else {
            auto fields = _desc->type()->members();
            _expressions.reserve(fields.size());
            for (auto i = 0u; i < fields.size(); i++) {
                if (std::find(soa_excluded.begin(), soa_excluded.end(), i) != soa_excluded.end()) {
                    _expressions.emplace_back(nullptr);
                } else {
                    auto type = i == 0u ? Type::array(Type::of<uint>(), 3u) : fields[i];
                    auto s = fb->shared(Type::array(type, _size));
                    _expressions.emplace_back(s);
                }
            }
        }
    }

public:
    Shared(luisa::shared_ptr<const coroutine::CoroFrameDesc> desc, size_t n,
           bool soa = true, luisa::span<const uint> soa_excluded_fields = {}) noexcept
        : _desc{std::move(desc)}, _size{n} { _create(soa, soa_excluded_fields); }

    Shared(Shared &&) noexcept = default;
    Shared(const Shared &) noexcept = delete;
    Shared &operator=(Shared &&) noexcept = delete;
    Shared &operator=(const Shared &) noexcept = delete;

    [[nodiscard]] auto desc() const noexcept { return _desc.get(); }
    [[nodiscard]] auto is_soa() const noexcept { return _expressions.size() > 1; }
    [[nodiscard]] auto size() const noexcept { return _size; }

public:
    /// Read index with active fields
    template<typename I>
    [[nodiscard]] auto read(I &&index, luisa::optional<luisa::span<const uint>> active_fields = luisa::nullopt) const noexcept {
        auto i = def(std::forward<I>(index));
        auto fb = detail::FunctionBuilder::current();
        auto frame = fb->local(_desc->type());
        if (!is_soa()) {
            auto expr = fb->access(_desc->type(), _expressions[0], i.expression());
            fb->assign(frame, expr);
        } else {
            auto fields = _desc->type()->members();
            for (auto m = 0u; m < fields.size(); m++) {
                if (_expressions[m] == nullptr) { continue; }
                if (active_fields && std::find(active_fields->begin(), active_fields->end(), m) == active_fields->end()) { continue; }
                auto f = fb->member(fields[m], frame, m);
                if (m == 0u) {
                    auto t = Type::of<uint>();
                    auto s = fb->access(Type::array(t, 3u), _expressions[m], i.expression());
                    std::array<const Expression *, 3u> elems;
                    elems[0] = fb->access(t, s, fb->literal(t, 0u));
                    elems[1] = fb->access(t, s, fb->literal(t, 1u));
                    elems[2] = fb->access(t, s, fb->literal(t, 2u));
                    auto v = fb->call(Type::of<uint3>(), CallOp::MAKE_UINT3, elems);
                    fb->assign(f, v);
                } else {
                    auto s = fb->access(fields[m], _expressions[m], i.expression());
                    fb->assign(f, s);
                }
            }
        }
        return coroutine::CoroFrame{_desc, frame};
    }

    /// Write index with active fields
    template<typename I>
    void write(I &&index, const coroutine::CoroFrame &frame, luisa::optional<luisa::span<const uint>> active_fields = luisa::nullopt) const noexcept {
        auto i = def(std::forward<I>(index));
        auto fb = detail::FunctionBuilder::current();
        if (!is_soa()) {
            auto ref = fb->access(_desc->type(), _expressions[0], i.expression());
            fb->assign(ref, frame.expression());
        } else {
            auto fields = _desc->type()->members();
            for (auto m = 0u; m < fields.size(); m++) {
                if (_expressions[m] == nullptr) { continue; }
                if (active_fields && std::find(active_fields->begin(), active_fields->end(), m) == active_fields->end()) { continue; }
                auto f = fb->member(fields[m], frame.expression(), m);
                if (m == 0u) {
                    auto t = Type::of<uint>();
                    auto s = fb->access(Type::array(t, 3u), _expressions[m], i.expression());
                    for (auto j = 0u; j < 3u; j++) {
                        auto fj = fb->swizzle(t, f, 1u, j);
                        auto sj = fb->access(t, s, fb->literal(t, j));
                        fb->assign(sj, fj);
                    }
                } else {
                    auto s = fb->access(fields[m], _expressions[m], i.expression());
                    fb->assign(s, f);
                }
            }
        }
    }
};

}// namespace luisa::compute
