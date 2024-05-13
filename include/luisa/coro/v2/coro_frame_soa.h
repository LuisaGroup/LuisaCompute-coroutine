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
    T<coroutine::CoroFrame> _soa;
    using CoroFrameSOA = T<coroutine::CoroFrame>;

public:
    LUISA_RESOURCE_PROXY_AVOID_CONSTRUCTION(CoroFrameSOAExprProxy)

public:
    template<typename I>
        requires is_integral_expr_v<I>
    [[nodiscard]] auto read(
        I &&index,
        luisa::optional<const luisa::span<const uint> &> active_fields = luisa::nullopt) const noexcept {
        return Expr<CoroFrameSOA>{_soa}.read(std::forward<I>(index), std::move(active_fields));
    }
    template<typename I, typename V>
        requires is_integral_expr_v<I>
    void write(
        I &&index, V &&value,
        luisa::optional<const luisa::span<const uint> &> active_fields = luisa::nullopt) const noexcept {
        Expr<CoroFrameSOA>{_soa}.write(std::forward<I>(index),
                                       std::forward<V>(value),
                                       std::move(active_fields));
    }
    [[nodiscard]] Expr<uint64_t> device_address() const noexcept {
        return Expr<CoroFrameSOA>{_soa}.device_address();
    }
};

}// namespace detail

template<>
class SOA<coroutine::CoroFrame> {

private:
    luisa::shared_ptr<const coroutine::CoroFrameDesc> _desc;
    size_t _size{0u};
    luisa::shared_ptr<luisa::vector<uint>> _field_offsets;
    size_t _buffer_element_count{0u};
    ByteBuffer _buffer;

private:
    [[nodiscard]] auto _init(DeviceInterface *device) noexcept {
        auto size_bytes = 0u;
        size_bytes = 0u;
        const auto type = _desc->type();
        auto fields = type->members();
        LUISA_ASSERT(_field_offsets == nullptr, "CoroFrame SOA Buffer already initialized.");
        _field_offsets = luisa::make_shared<luisa::vector<uint>>();
        _field_offsets->reserve(fields.size());
        for (const auto field : fields) {
            size_bytes = (size_bytes + field->alignment() - 1u) & ~(field->alignment() - 1u);
            _field_offsets->emplace_back(size_bytes);
            size_bytes += field->size() * _size;
        }
        _buffer_element_count = (size_bytes + element_type()->size() - 1u) / element_type()->size();
        return device->create_buffer(
            element_type(),
            _buffer_element_count,
            nullptr);
    }

public:
    [[nodiscard]] static const Type *element_type() noexcept {
        return Type::of<uint>();
    }

public:
    SOA(DeviceInterface *device, luisa::shared_ptr<const coroutine::CoroFrameDesc> desc, size_t n, bool soa) noexcept
        : _size{n}, _desc{std::move(desc)} {
        auto info = _init(device);
        _buffer = std::move(ByteBuffer{device, info});
        LUISA_ASSERT(_buffer.size_bytes() == _buffer_element_count * element_type()->size(),
                     "CoroFrame SOA Buffer size mismatch.");
    }
    SOA() noexcept = default;
    SOA(const SOA &) = delete;
    SOA(SOA &&) noexcept = default;
    SOA &operator=(const SOA &) = delete;
    SOA &operator=(SOA &&x) noexcept {
        _desc = std::move(x._desc);
        _size = x._size;
        _field_offsets = std::move(x._field_offsets);
        _buffer_element_count = x._buffer_element_count;
        _buffer = std::move(x._buffer);
        return *this;
    }
    ~SOA() noexcept = default;

    // properties
    [[nodiscard]] auto desc() const noexcept { return _desc.get(); }
    [[nodiscard]] auto size() const noexcept { return _size; }
    [[nodiscard]] auto handle() const noexcept { return _buffer.handle(); }
    [[nodiscard]] auto size_bytes() const noexcept { return _buffer.size_bytes(); }
    [[nodiscard]] auto view() const noexcept { return SOAView<coroutine::CoroFrame>{_desc, _buffer.view()}; }
    // DSL interface
    [[nodiscard]] auto operator->() const noexcept {
        return reinterpret_cast<const detail::CoroFrameSOAExprProxy<SOA<coroutine::CoroFrame>> *>(this);
    }
};

template<>
class SOAView<coroutine::CoroFrame> {
private:
    luisa::shared_ptr<const coroutine::CoroFrameDesc> _desc;
    ByteBufferView _buffer_view;
    luisa::shared_ptr<luisa::vector<uint>> _field_offsets;

private:
    friend class SOA<coroutine::CoroFrame>;

public:
    SOAView() noexcept = default;
    SOAView(luisa::shared_ptr<const coroutine::CoroFrameDesc> desc, ByteBufferView buffer_view,
            luisa::shared_ptr<luisa::vector<uint>> field_offsets) noexcept
        : _desc{std::move(desc)},
          _buffer_view{buffer_view},
          _field_offsets{field_offsets} {}
    SOAView(const SOA<coroutine::CoroFrame> &soa) noexcept
        : SOAView{soa.view()} {}
    ~SOAView() noexcept = default;

    [[nodiscard]] auto desc() const noexcept { return _desc.get(); }
    [[nodiscard]] auto handle() const noexcept { return _buffer_view.handle(); }
    [[nodiscard]] auto offset() const noexcept { return _buffer_view.offset(); }
    [[nodiscard]] auto size_bytes() const noexcept { return _buffer_view.size_bytes(); }
    [[nodiscard]] auto total_size() const noexcept { return _buffer_view.total_size(); }
    [[nodiscard]] auto field_offsets() const noexcept { return _field_offsets; }
    // DSL interface
    [[nodiscard]] auto operator->() const noexcept {
        return reinterpret_cast<const detail::CoroFrameSOAExprProxy<SOA<coroutine::CoroFrame>> *>(this);
    }
};

/// Class of Expr<SOA<coroutine::CoroFrame>>
template<>
struct Expr<SOA<coroutine::CoroFrame>> {

private:
    luisa::shared_ptr<const coroutine::CoroFrameDesc> _desc;
    luisa::shared_ptr<luisa::vector<uint>> _field_offsets;
    const RefExpr *_expression{nullptr};

public:
    /// Construct from RefExpr
    explicit Expr(const RefExpr *expr) noexcept
        : _expression{expr} {}

    /// Construct from SOAView<coroutine::CoroFrame>. Will call buffer_binding() to bind buffer
    Expr(const SOAView<coroutine::CoroFrame> &soa_view) noexcept
        : _desc{soa_view.desc()->shared_from_this()},
          _expression{detail::FunctionBuilder::current()->buffer_binding(
              Type::buffer(SOA<coroutine::CoroFrame>::element_type()), soa_view.handle(),
              soa_view.offset(), soa_view.size_bytes())},
          _field_offsets{soa_view.field_offsets()} {
        LUISA_ASSERT((soa_view.offset() == 0u) && (soa_view.size_bytes() == soa_view.total_size()),
            "The ByteBufferView in SOAView must be an original view.");
    }

    /// Construct from SOA<coroutine::CoroFrame>. Will call buffer_binding() to bind buffer
    Expr(const SOA<coroutine::CoroFrame> &soa) noexcept
        : Expr{SOAView<coroutine::CoroFrame>{soa}} {}

    /// Construct from Var<SOA<coroutine::CoroFrame>>.
    template<typename T>
        requires std::same_as<T, Var<coroutine::CoroFrame>>
    Expr(const T &soa) noexcept
        : Expr{soa.desc()->shread, soa.expression()} {}

    /// Construct from Var<SOAView<coroutine::CoroFrame>>.
    template<typename T>
        requires std::same_as<T, Var<SOAView<coroutine::CoroFrame>>>
    Expr(const T &soa_view) noexcept
        : Expr{soa_view.expression()} {}

    /// Return RefExpr
    [[nodiscard]] const RefExpr *expression() const noexcept { return _expression; }

    /// Read index with active fields
    template<typename I>
    [[nodiscard]] auto read(
        I &&index,
        luisa::optional<const luisa::span<const uint> &> active_fields = luisa::nullopt) const noexcept {
        auto fb = detail::FunctionBuilder::current();
        auto frame = fb->local(_desc->type());
        auto fields = _desc->type()->members();
        for (auto i = 0u; i < fields.size(); i++) {
            if (active_fields && std::find(active_fields->begin(), active_fields->end(), i) == active_fields->end()) { continue; }
            auto field = fields[i];
            auto offset = _field_offsets->at(i);
            auto f = fb->member(field, frame, i);
            auto offset_var = offset + index * field->size();
            auto s = fb->call(
                field, CallOp::BUFFER_READ,
                {_expression, detail::extract_expression(offset_var)});
            fb->assign(f, s);
        }
        return coroutine::CoroFrame{_desc, frame};
    }

    /// Write index with active fields
    template<typename I>
    void write(
        I &&index, const coroutine::CoroFrame &frame,
        luisa::optional<const luisa::span<const uint> &> active_fields = luisa::nullopt) const noexcept {
        auto fb = detail::FunctionBuilder::current();
        auto fields = _desc->type()->members();
        for (auto i = 0u; i < fields.size(); i++) {
            if (active_fields && std::find(active_fields->begin(), active_fields->end(), i) == active_fields->end()) { continue; }
            auto field = fields[i];
            auto offset = _field_offsets->at(i);
            auto offset_var = offset + index * field->size();
            auto f = fb->member(field, frame.expression(), i);
            auto s = fb->call(
                field, CallOp::BUFFER_WRITE,
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

/// Class of Var<SOA<coroutine::CoroFrame>>
template<>
struct Var<SOA<coroutine::CoroFrame>> : public Expr<SOA<coroutine::CoroFrame>> {
    explicit Var(detail::ArgumentCreation) noexcept
        : Expr{detail::FunctionBuilder::current()->buffer(SOA<coroutine::CoroFrame>::element_type())} {}
    Var(Var &&) noexcept = default;
    Var(const Var &) noexcept = delete;
};

}// namespace luisa::compute