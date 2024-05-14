#pragma once

#include "luisa/coro/v2/coro_frame.h"
#include <luisa/runtime/buffer.h>

namespace luisa::compute {

namespace detail {
LC_RUNTIME_API void error_buffer_size_not_aligned(size_t align) noexcept;
template<typename BufferOrExpr>
class ByteBufferExprProxy;
}// namespace detail

template<typename T>
class SOA;

class ByteBufferView;

class LC_RUNTIME_API ByteBuffer final : public Resource {

private:
    size_t _size_bytes{};

private:
    friend class Device;
    friend class ResourceGenerator;
    friend class SOA<coroutine::CoroFrame>;
    ByteBuffer(DeviceInterface *device, const BufferCreationInfo &info) noexcept;
    ByteBuffer(DeviceInterface *device, size_t size_bytes) noexcept;

public:
    [[nodiscard]] auto size_bytes() const noexcept { return _size_bytes; }
    ByteBuffer() noexcept = default;
    ~ByteBuffer() noexcept override;
    ByteBuffer(ByteBuffer &&) noexcept = default;
    ByteBuffer(ByteBuffer const &) noexcept = delete;
    ByteBuffer &operator=(ByteBuffer &&rhs) noexcept {
        _move_from(std::move(rhs));
        return *this;
    }
    ByteBuffer &operator=(ByteBuffer const &) noexcept = delete;
    [[nodiscard]] ByteBufferView view() const noexcept;
    using Resource::operator bool;
    [[nodiscard]] auto copy_to(void *data) const noexcept {
        _check_is_valid();
        return luisa::make_unique<BufferDownloadCommand>(handle(), 0u, _size_bytes, data);
    }
    [[nodiscard]] auto copy_from(const void *data) noexcept {
        _check_is_valid();
        return luisa::make_unique<BufferUploadCommand>(handle(), 0u, _size_bytes, data);
    }
    [[nodiscard]] auto copy_from(const void *data, size_t buffer_offset, size_t size_bytes) noexcept {
        _check_is_valid();
        if (size_bytes > _size_bytes) [[unlikely]] {
            detail::error_buffer_copy_sizes_mismatch(size_bytes, _size_bytes);
        }
        return luisa::make_unique<BufferUploadCommand>(handle(), buffer_offset, size_bytes, data);
    }
    template<typename T>
    [[nodiscard]] auto copy_from(BufferView<T> source) noexcept {
        _check_is_valid();
        if (source.size_bytes() != _size_bytes) [[unlikely]] {
            detail::error_buffer_copy_sizes_mismatch(source.size_bytes(), _size_bytes);
        }
        return luisa::make_unique<BufferCopyCommand>(
            source.handle(), this->handle(),
            source.offset_bytes(), 0u,
            this->size_bytes());
    }
    [[nodiscard]] auto copy_from(const ByteBuffer &source, size_t offset, size_t size_bytes) noexcept {
        _check_is_valid();
        if (size_bytes > _size_bytes) [[unlikely]] {
            detail::error_buffer_copy_sizes_mismatch(size_bytes, _size_bytes);
        }
        return luisa::make_unique<BufferCopyCommand>(
            source.handle(), this->handle(),
            offset, 0u,
            size_bytes);
    }
    // DSL interface
    [[nodiscard]] auto operator->() const noexcept {
        _check_is_valid();
        return reinterpret_cast<const detail::ByteBufferExprProxy<ByteBuffer> *>(this);
    }
};

class ByteBufferView {

private:
    void *_native_handle;
    uint64_t _handle;
    size_t _offset_bytes;
    size_t _size;
    size_t _total_size;

private:
    friend class ByteBuffer;

public:
    ByteBufferView(void *native_handle, uint64_t handle,
                   size_t offset_bytes,
                   size_t size, size_t total_size) noexcept
        : _native_handle{native_handle}, _handle{handle}, _offset_bytes{offset_bytes},
          _size{size}, _total_size{total_size} {}

    ByteBufferView(const ByteBuffer &buffer) noexcept : ByteBufferView{buffer.view()} {}
    ByteBufferView(const ByteBufferView &) noexcept = default;
    ByteBufferView(ByteBufferView &&) noexcept = default;
    ByteBufferView() noexcept : ByteBufferView{nullptr, invalid_resource_handle, 0u, 0u, 0u} {}
    [[nodiscard]] explicit operator bool() const noexcept { return _handle != invalid_resource_handle; }

    [[nodiscard]] auto handle() const noexcept { return _handle; }
    [[nodiscard]] auto native_handle() const noexcept { return _native_handle; }
    [[nodiscard]] auto offset() const noexcept { return _offset_bytes; }
    [[nodiscard]] auto size_bytes() const noexcept { return _size; }
    [[nodiscard]] auto total_size() const noexcept { return _total_size; }
    [[nodiscard]] auto original() const noexcept {
        return ByteBufferView{_native_handle, _handle, 0u, _total_size, _total_size};
    }
    [[nodiscard]] auto subview(size_t offset_elements, size_t size_elements) const noexcept {
        if (size_elements + offset_elements > _size) [[unlikely]] {
            detail::error_buffer_subview_overflow(offset_elements, size_elements, _size);
        }
        return ByteBufferView{_native_handle, _handle, _offset_bytes + offset_elements, size_elements, _total_size};
    }
    // reinterpret cast buffer to another type U
    template<typename U>
        requires(!is_custom_struct_v<U>)
    [[nodiscard]] auto as() const noexcept {
        if (this->size_bytes() < sizeof(U)) [[unlikely]] {
            detail::error_buffer_reinterpret_size_too_small(sizeof(U), this->size_bytes());
        }
        auto total_size_bytes = _total_size;
        return BufferView<U>{_native_handle, _handle, sizeof(U), _offset_bytes,
                             this->size_bytes() / sizeof(U), total_size_bytes / sizeof(U)};
    }
    // DSL interface
    [[nodiscard]] auto operator->() const noexcept {
        return reinterpret_cast<const detail::ByteBufferExprProxy<ByteBufferView> *>(this);
    }
};

namespace detail {

template<>
struct is_buffer_impl<ByteBuffer> : std::true_type {};

}// namespace detail

}// namespace luisa::compute
