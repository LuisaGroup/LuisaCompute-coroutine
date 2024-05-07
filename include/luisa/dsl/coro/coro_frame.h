//
// Created by mike on 5/7/24.
//

#pragma once

#include <luisa/core/dll_export.h>
#include <luisa/core/stl/string.h>
#include <luisa/core/stl/unordered_map.h>
#include <luisa/ast/type.h>

namespace luisa::compute::inline dsl::coro_v2 {

class CoroFrame;

class LC_DSL_API CoroFrameDesc : public luisa::enable_shared_from_this<CoroFrameDesc> {

public:
    using DesignatedMemberDict = luisa::unordered_map<luisa::string, uint>;

private:
    const Type *_type{nullptr};
    DesignatedMemberDict _designated_members;

private:
    CoroFrameDesc(const Type *type, DesignatedMemberDict m) noexcept;

public:
    [[nodiscard]] static luisa::shared_ptr<CoroFrameDesc> create(const Type *type, DesignatedMemberDict m) noexcept;
    [[nodiscard]] auto type() const noexcept { return _type; }
    [[nodiscard]] auto &designated_members() const noexcept { return _designated_members; }
    [[nodiscard]] uint designated_member(luisa::string_view name) const noexcept;

public:
    [[nodiscard]] CoroFrame instantiate() const noexcept;
    [[nodiscard]] CoroFrame instantiate(Expr<uint3> coro_id) const noexcept;
};

class LC_DSL_API CoroFrame {

private:
    luisa::shared_ptr<const CoroFrameDesc> _desc;
    const RefExpr *_expression;

public:
    Var<uint3> &coro_id;
    Var<uint> &target_token;

public:
    CoroFrame(luisa::shared_ptr<const CoroFrameDesc> desc, const RefExpr *expr) noexcept;
    CoroFrame(CoroFrame &&another) noexcept;
    CoroFrame(const CoroFrame &another) noexcept;
    CoroFrame &operator=(const CoroFrame &rhs) noexcept;
    CoroFrame &operator=(CoroFrame &&rhs) noexcept;

private:
    void _check_member_index(uint index) const noexcept;

public:
    [[nodiscard]] auto description() const noexcept { return _desc.get(); }
    [[nodiscard]] auto expression() const noexcept { return _expression; }

public:
    template<typename T>
    [[nodiscard]] Var<T> &m(uint index) noexcept {
        _check_member_index(index);
        auto fb = detail::FunctionBuilder::current();
        auto member = fb->member(_desc->type()->members()[index], _expression, index);
        return *fb->create_temporary<Var<T>>(member);
    }
    template<typename T>
    [[nodiscard]] const Var<T> &m(uint index) const noexcept {
        return const_cast<CoroFrame *>(this)->m<T>(index);
    }
    template<typename T>
    [[nodiscard]] Var<T> &m(luisa::string_view name) noexcept {
        return m<T>(_desc->designated_member(name));
    }
    template<typename T>
    [[nodiscard]] const Var<T> &m(luisa::string_view name) const noexcept {
        return const_cast<CoroFrame *>(this)->m<T>(name);
    }
};

}// namespace luisa::compute::inline dsl::coro_v2
