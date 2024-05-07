//
// Created by mike on 5/7/24.
//

#include <luisa/core/logging.h>
#include <luisa/dsl/coro/coro_frame.h>

namespace luisa::compute::inline dsl::coro_v2 {

CoroFrameDesc::CoroFrameDesc(const Type *type, DesignatedMemberDict m) noexcept
    : _type{type}, _designated_members{std::move(m)} {
    LUISA_ASSERT(_type != nullptr, "CoroFrame underlying type must not be null.");
    LUISA_ASSERT(_type->is_structure(), "CoroFrame underlying type must be a structure.");
    LUISA_ASSERT(_type->members().size() >= 2u, "CoroFrame underlying type must have at least 2 members (coro_id and target_token).");
    LUISA_ASSERT(_type->members()[0] == Type::of<uint3>(), "CoroFrame member 0 (coro_id) must be uint3.");
    LUISA_ASSERT(_type->members()[1] == Type::of<uint>(), "CoroFrame member 1 (target_token) must be uint.");
    auto member_count = _type->members().size();
    for (auto &&[name, index] : _designated_members) {
        LUISA_ASSERT(name != "coro_id", "CoroFrame designated member name 'coro_id' is reserved.");
        LUISA_ASSERT(name != "target_token", "CoroFrame designated member name 'target_token' is reserved.");
        LUISA_ASSERT(index != 0, "CoroFrame designated member index 0 is reserved for coro_id.");
        LUISA_ASSERT(index != 1, "CoroFrame designated member index 1 is reserved for target_token.");
        LUISA_ASSERT(index < member_count, "CoroFrame designated member index out of range.");
    }
}

luisa::shared_ptr<CoroFrameDesc> CoroFrameDesc::create(const Type *type, DesignatedMemberDict m) noexcept {
    return luisa::make_shared<CoroFrameDesc>(CoroFrameDesc{type, std::move(m)});
}

uint CoroFrameDesc::designated_member(luisa::string_view name) const noexcept {
    if (name == "coro_id") { return 0u; }
    if (name == "target_token") { return 1u; }
    auto iter = _designated_members.find(name);
    LUISA_ASSERT(iter != _designated_members.end(), "CoroFrame designated member not found.");
    return iter->second;
}

bool CoroFrameDesc::operator==(const CoroFrameDesc &rhs) const noexcept {
    return this->type() == rhs.type() && this->designated_members() == rhs.designated_members();
}

CoroFrame CoroFrameDesc::instantiate(Expr<uint3> coro_id) const noexcept {
    auto fb = detail::FunctionBuilder::current();
    // create an variable for the coro frame
    auto expr = fb->local(_type);
    // initialize the coro frame members
    auto zero_init = fb->call(_type, CallOp::ZERO, {});
    fb->assign(expr, zero_init);
    // set the coro_id and target_token fields
    auto i = fb->member(_type->members()[0], expr, 0u);
    fb->assign(i, coro_id.expression());
    auto zero = fb->call(Type::of<uint>(), CallOp::ZERO, {});
    auto target_token = fb->member(_type->members()[1], expr, 1u);
    fb->assign(target_token, zero);
    return CoroFrame{shared_from_this(), expr};
}

CoroFrame::CoroFrame(luisa::shared_ptr<const CoroFrameDesc> desc, const RefExpr *expr) noexcept
    : _desc{std::move(desc)}, _expression{expr} {
    LUISA_ASSERT(expr != nullptr, "CoroFrame expression must not be null.");
    LUISA_ASSERT(expr->type() == _desc->type(), "CoroFrame expression type mismatch.");
}

CoroFrame::CoroFrame(CoroFrame &&another) noexcept {
    LUISA_NOT_IMPLEMENTED();
}

CoroFrame::CoroFrame(const CoroFrame &another) noexcept {
    LUISA_NOT_IMPLEMENTED();
}

CoroFrame &CoroFrame::operator=(const CoroFrame &rhs) noexcept {
    LUISA_NOT_IMPLEMENTED();
}

CoroFrame &CoroFrame::operator=(CoroFrame &&rhs) noexcept {
    LUISA_NOT_IMPLEMENTED();
}

}// namespace luisa::compute::inline dsl::coro_v2
