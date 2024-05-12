//
// Created by ChenXin on 2024/5/12.
//

#pragma once

#include "luisa/runtime/device.h"
#include "spdlog/fmt/bundled/compile.h"

#include <luisa/runtime/byte_buffer.h>
#include <luisa/dsl/resource.h>
#include <luisa/coro/v2/coro_frame.h>

namespace luisa::compute {

template<>
class SOA<coroutine::CoroFrame> {

private:
    luisa::shared_ptr<const coroutine::CoroFrameDesc> _desc;
    size_t _count{0u};
    ByteBuffer _buffer;
    luisa::vector<uint> _field_offsets;

public:
    SOA(Device &device, luisa::shared_ptr<const coroutine::CoroFrameDesc> desc, size_t n) noexcept
        : _desc{std::move(desc)}, _count{n} {
        auto size_bytes = 0u;
        const auto type = _desc->type();
        auto fields = type->members();
        for (auto field : fields) {
            auto aligned_offset = (size_bytes + field->alignment() - 1u) & ~(field->alignment() - 1u);
            _field_offsets.emplace_back(aligned_offset);
            size_bytes = aligned_offset;
            size_bytes += field->size() * _count;
        }
        _buffer = device.create_byte_buffer(size_bytes);
    }
    SOA() noexcept = default;
    SOA(const SOA &) = delete;
    SOA(SOA &&) noexcept = default;
    SOA &operator=(const SOA &) = delete;
    SOA &operator=(SOA &&) noexcept = default;
    ~SOA() noexcept = default;

private:
    /// Read index with active fields
    template<typename I>
    [[nodiscard]] auto _read(I &&index, luisa::optional<luisa::span<const uint>> active_fields) const noexcept {
        auto fb = detail::FunctionBuilder::current();
        auto frame = fb->local(_desc->type());
        auto fields = _desc->type()->members();
        for (auto i = 0u; i < fields.size(); i++) {
            if (active_fields && std::find(active_fields->begin(), active_fields->end(), i) == active_fields->end()) { continue; }
            auto field = fields[i];
            auto offset = _field_offsets[i];
            auto field_buffer = fb->buffer_binding(field, _buffer.handle(), offset, field->size() * _count);
            auto f = fb->member(field, frame, i);
            auto s = fb->call(
                field, CallOp::BUFFER_READ,
                {field_buffer, detail::extract_expression(std::forward<I>(index))});
            fb->assign(f, s);
        }
        return coroutine::CoroFrame{_desc, frame};
    }

    /// Write index with active fields
    template<typename I>
    void _write(I &&index, const coroutine::CoroFrame &frame, luisa::optional<luisa::span<const uint>> active_fields) const noexcept {
        auto fb = detail::FunctionBuilder::current();
        auto fields = _desc->type()->members();
        for (auto i = 0u; i < fields.size(); i++) {
            if (active_fields && std::find(active_fields->begin(), active_fields->end(), i) == active_fields->end()) { continue; }
            auto field = fields[i];
            auto offset = _field_offsets[i];
            auto field_buffer = fb->buffer_binding(field, _buffer.handle(), offset, field->size() * _count);
            auto f = fb->member(field, frame.expression(), i);
            auto s = fb->call(
                field, CallOp::BUFFER_WRITE,
                {field_buffer, detail::extract_expression(std::forward<I>(index)), f});
        }
    }

public:
    template<typename I>
    [[nodiscard]] auto read(I &&index) const noexcept {
        return _read(std::forward<I>(index), luisa::nullopt);
    }
    template<typename I>
    [[nodiscard]] auto read(I &&index, luisa::span<const uint> active_fields) const noexcept {
        return _read(std::forward<I>(index), luisa::make_optional(active_fields));
    }
    template<typename I>
    void write(I &&index, const coroutine::CoroFrame &frame) const noexcept {
        _write(std::forward<I>(index), frame, luisa::nullopt);
    }
    template<typename I>
    void write(I &&index, const coroutine::CoroFrame &frame, luisa::span<const uint> active_fields) const noexcept {
        _write(std::forward<I>(index), frame, luisa::make_optional(active_fields));
    }
};

}// namespace luisa::compute