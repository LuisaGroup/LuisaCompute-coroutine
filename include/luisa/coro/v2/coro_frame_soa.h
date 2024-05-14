//
// Created by ChenXin on 2024/5/12.
//

#pragma once

#include "luisa/runtime/device.h"
#include "spdlog/fmt/bundled/compile.h"

#include <luisa/runtime/byte_buffer.h>
#include <luisa/dsl/resource.h>
#include <luisa/coro/v2/coro_frame.h>

#include <utility>

namespace luisa::compute {

namespace detail {

template<typename T>
class CoroFrameSOAExprProxy {

private:
    T _soa;

public:
    LUISA_RESOURCE_PROXY_AVOID_CONSTRUCTION(CoroFrameSOAExprProxy)

public:
    template<typename I>
        requires is_integral_expr_v<I>
    [[nodiscard]] auto read(
        I &&index,
        luisa::optional<luisa::span<const uint>> active_fields = luisa::nullopt) const noexcept {
        return Expr<T>{_soa}.read(std::forward<I>(index), std::move(active_fields));
    }
    template<typename I, typename V>
        requires is_integral_expr_v<I>
    void write(
        I &&index, V &&value,
        luisa::optional<luisa::span<const uint>> active_fields = luisa::nullopt) const noexcept {
        Expr<T>{_soa}.write(std::forward<I>(index),
                                       std::forward<V>(value),
                                       std::move(active_fields));
    }
    [[nodiscard]] Expr<uint64_t> device_address() const noexcept {
        return Expr<T>{_soa}.device_address();
    }
};

}// namespace detail

template<typename T>
class SOA;

struct SOABase {
protected:
    luisa::shared_ptr<const coroutine::CoroFrameDesc> _desc;
    luisa::shared_ptr<luisa::vector<uint>> _field_offsets;
    uint _range_start{0u}, _size{0u};

public:
    SOABase() noexcept = default;
    SOABase(luisa::shared_ptr<const coroutine::CoroFrameDesc> desc,
            luisa::shared_ptr<luisa::vector<uint>> field_offsets,
            uint range_start, uint size) noexcept
        : _desc{std::move(desc)},
          _field_offsets{std::move(field_offsets)},
          _range_start{range_start}, _size{size} {}
};

template<>
class SOAView<coroutine::CoroFrame> : public SOABase {
private:
    ByteBufferView _buffer_view;

public:
    SOAView() noexcept = default;
    SOAView(luisa::shared_ptr<const coroutine::CoroFrameDesc> desc, ByteBufferView buffer_view,
            luisa::shared_ptr<luisa::vector<uint>> field_offsets,
            uint range_start, uint size) noexcept
        : SOABase{std::move(desc), field_offsets, range_start, size},
          _buffer_view{buffer_view} {
        LUISA_ASSERT((_buffer_view.offset() == 0u) &&
                         (_buffer_view.size_bytes() == _buffer_view.total_size()),
                     "Invalid buffer view for SOA.");
    }
    template<typename T>
        requires std::same_as<T, SOA<coroutine::CoroFrame>>
    SOAView(const T &soa) noexcept
        : SOAView{soa.view()} {}
    ~SOAView() noexcept = default;

    [[nodiscard]] auto desc() const noexcept { return _desc.get(); }
    [[nodiscard]] auto handle() const noexcept { return _buffer_view.handle(); }
    [[nodiscard]] auto range_start() const noexcept { return _range_start; }
    [[nodiscard]] auto size() const noexcept { return _size; }
    [[nodiscard]] auto size_bytes() const noexcept { return _buffer_view.size_bytes(); }
    [[nodiscard]] auto field_offsets() const noexcept { return _field_offsets; }
    // DSL interface
    [[nodiscard]] auto operator->() const noexcept {
        return reinterpret_cast<const detail::CoroFrameSOAExprProxy<SOAView<coroutine::CoroFrame>> *>(this);
    }
};



template<>
class SOA<coroutine::CoroFrame> : public SOABase {

private:
    ByteBuffer _buffer;

public:
    SOA(DeviceInterface *device, luisa::shared_ptr<const coroutine::CoroFrameDesc> desc, uint n, bool soa) noexcept
        : SOABase{std::move(desc), luisa::make_shared<luisa::vector<uint>>(), 0u, n} {
        auto size_bytes = 0u;
        size_bytes = 0u;
        auto fields = _desc->type()->members();
        _field_offsets->reserve(fields.size());
        for (const auto field : fields) {
            size_bytes = (size_bytes + field->alignment() - 1u) & ~(field->alignment() - 1u);
            _field_offsets->emplace_back(size_bytes);
            if (field->size() % field->alignment() != 0u) [[unlikely]] {
                detail::error_buffer_invalid_alignment(size_bytes + field->size(), field->alignment());
            }
            size_bytes += field->size() * _size;
        }
        auto buffer_element_count = (size_bytes + sizeof(uint) - 1u) / sizeof(uint);
        auto info = device->create_buffer(
            Type::of<uint>(),
            buffer_element_count,
            nullptr);
        _buffer = std::move(ByteBuffer{device, info});
    }
    SOA() noexcept = default;
    SOA(const SOA &) = delete;
    SOA(SOA &&) noexcept = default;
    SOA &operator=(const SOA &) = delete;
    SOA &operator=(SOA &&x) noexcept {
        _desc = std::move(x._desc);
        _size = x._size;
        _field_offsets = std::move(x._field_offsets);
        _buffer = std::move(x._buffer);
        return *this;
    }
    ~SOA() noexcept = default;

    // properties
    [[nodiscard]] auto desc() const noexcept { return _desc.get(); }
    [[nodiscard]] auto view() const noexcept {
        return SOAView<coroutine::CoroFrame>{
            _desc,
            _buffer.view(),
            _field_offsets,
            0u,
            _size};
    }
    // DSL interface
    [[nodiscard]] auto operator->() const noexcept {
        return reinterpret_cast<const detail::CoroFrameSOAExprProxy<SOA<coroutine::CoroFrame>> *>(this);
    }
};

/// Class of Expr<SOA<coroutine::CoroFrame>>
template<>
struct Expr<SOA<coroutine::CoroFrame>> : SOABase {

private:
    const RefExpr *_expression{nullptr};

public:
    /// Construct from SOAView<coroutine::CoroFrame>. Will call buffer_binding() to bind buffer
    Expr(const SOAView<coroutine::CoroFrame> &soa_view) noexcept
        : SOABase{soa_view.desc()->shared_from_this(), soa_view.field_offsets(),
                  soa_view.range_start(), soa_view.size()},
          _expression{detail::FunctionBuilder::current()->buffer_binding(
              Type::buffer(Type::of<ByteBuffer>()), soa_view.handle(),
              0u, soa_view.size_bytes())} {}

    /// Construct from SOA<coroutine::CoroFrame>. Will call buffer_binding() to bind buffer
    Expr(const SOA<coroutine::CoroFrame> &soa) noexcept
        : Expr{SOAView<coroutine::CoroFrame>{soa}} {}

    /// Return RefExpr
    [[nodiscard]] const RefExpr *expression() const noexcept { return _expression; }

    /// Read index with active fields
    template<typename I>
    [[nodiscard]] auto read(
        I &&index,
        luisa::optional<luisa::span<const uint>> active_fields = luisa::nullopt) const noexcept {
        auto fb = detail::FunctionBuilder::current();
        auto frame = fb->local(_desc->type());
        auto fields = _desc->type()->members();
        for (auto i = 0u; i < fields.size(); i++) {
            if (active_fields && std::find(active_fields->begin(), active_fields->end(), i) == active_fields->end()) { continue; }
            auto field_type = fields[i];
            auto offset = _field_offsets->at(i);
            auto f = fb->member(field_type, frame, i);
            auto offset_var = offset + index * field_type->size();
            auto s = fb->call(
                field_type, CallOp::BYTE_BUFFER_READ,
                {_expression, detail::extract_expression(offset_var)});
            fb->assign(f, s);
        }
        return coroutine::CoroFrame{_desc, frame};
    }

    /// Write index with active fields
    template<typename I>
    void write(
        I &&index, const coroutine::CoroFrame &frame,
        luisa::optional<luisa::span<const uint>> active_fields = luisa::nullopt) const noexcept {
        auto fb = detail::FunctionBuilder::current();
        auto fields = _desc->type()->members();
        for (auto i = 0u; i < fields.size(); i++) {
            if (active_fields && std::find(active_fields->begin(), active_fields->end(), i) == active_fields->end()) { continue; }
            auto field_type = fields[i];
            auto offset = _field_offsets->at(i);
            auto offset_var = offset + index * field_type->size();
            auto f = fb->member(field_type, frame.expression(), i);
            auto s = fb->call(
                field_type, CallOp::BYTE_BUFFER_WRITE,
                {_expression, detail::extract_expression(offset_var), f});
        }
    }

    [[nodiscard]] Expr<uint64_t> device_address() const noexcept {
        return def<uint64_t>(detail::FunctionBuilder::current()->call(
            Type::of<uint64_t>(), CallOp::BUFFER_ADDRESS, {_expression}));
    }

    /// Self-pointer to unify the interfaces of the captured Buffer<T> and Expr<Buffer<T>>
    [[nodiscard]] auto operator->() const noexcept { return this; }
};

/// Class of Expr<SOAView<coroutine::CoroFrame>>
template<>
struct Expr<SOAView<coroutine::CoroFrame>> : public Expr<SOA<coroutine::CoroFrame>> {
    using Expr<SOA<coroutine::CoroFrame>>::Expr;
};

}// namespace luisa::compute