//
// Created by ChenXin on 2023/6/29.
//

#pragma once

#include <luisa/runtime/shader.h>

#include <luisa/dsl/syntax.h>
#include <luisa/dsl/sugar.h>
#include <luisa/dsl/builtin.h>
#include <luisa/runtime/command_buffer.h>
#include <luisa/core/logging.h>
#include <luisa/runtime/rhi/argument.h>

namespace luisa::compute {

class LC_RUNTIME_API RayQueue {

public:
    static constexpr auto counter_buffer_size = 16u * 1024u;

private:
    Buffer<uint> _index_buffer;
    Buffer<uint> _counter_buffer;
    uint _current_counter;
    Shader1D<> _clear_counters;

public:
    RayQueue(Device &device, size_t size) noexcept;
    [[nodiscard]] BufferView<uint> prepare_counter_buffer(CommandBuffer &command_buffer) noexcept;
    [[nodiscard]] BufferView<uint> prepare_index_buffer(CommandBuffer &command_buffer) noexcept;
};

class LC_RUNTIME_API AggregatedRayQueue {
private:
    Buffer<uint> _index_buffer;
    Buffer<uint> _counter_buffer;
    Shader1D<> _clear_counters;
    uint _kernel_count;
    luisa::vector<uint> _host_counter;
    luisa::vector<uint> _offsets;
    size_t _state_count;
    bool _gathering;

public:
    AggregatedRayQueue(Device &device, size_t state_count, uint kernel_count, bool gathering) noexcept;
    void clear_counter_buffer(CommandBuffer &command_buffer, int index = -1) noexcept;
    [[nodiscard]] BufferView<uint> counter_buffer(uint index) noexcept;
    [[nodiscard]] BufferView<uint> index_buffer(uint index) noexcept;
    [[nodiscard]] uint host_counter(uint index) const noexcept;
    void catch_counter(CommandBuffer &command_buffer) noexcept;
};

struct LC_RUNTIME_API ArgumentInfo {

    struct Uniform {
        const void *data;
        size_t size;
    };

    using Buffer = Argument::Buffer;
    using Texture = Argument::Texture;
    using BindlessArray = Argument::BindlessArray;
    using Accel = Argument::Accel;
    using Tag = Argument::Tag;

    Tag tag;
    union {
        Buffer buffer;
        Texture texture;
        Uniform uniform;
        BindlessArray bindless_array;
        Accel accel;
    };
};

class LC_RUNTIME_API KernelInfo {

private:
    uint _shader_handle;
    uint _uniform_size;
    luisa::vector<ArgumentInfo> _args;

    luisa::optional<ShaderDispatchCmdEncoder> _encoder;

public:
    KernelInfo(uint shader_handle) noexcept : _uniform_size{0u} {}

    [[nodiscard]] auto dispatch() noexcept;
    void encode_uniform(const void *data, size_t size) noexcept;
    void encode_buffer(uint64_t handle, size_t offset, size_t size) noexcept;
    void encode_texture(uint64_t handle, uint32_t level) noexcept;
    void encode_bindless_array(uint64_t handle) noexcept;
    void encode_accel(uint64_t handle) noexcept;
};

class LC_RUNTIME_API ShaderScheduler {

private:
    AggregatedRayQueue _aggregated_kernel_queue;
    luisa::unique_ptr<luisa::function<void(uint)>> _launch_kernel;
    luisa::unordered_map<int, KernelInfo> _kernel_info;
    Device &_device;
    bool _gathering;

    bool _queue_empty;

public:
    ShaderScheduler(Device &device, size_t state_count, uint kernel_count, bool gathering) noexcept
        : _device{device}, _aggregated_kernel_queue{device, state_count, kernel_count, gathering},
          _gathering{gathering}, _queue_empty{true} {}

    template<typename Size>
        requires luisa::is_uint_vector_v<Size>
    void execute(CommandBuffer &command_buffer, Size dispatch_size) noexcept {
        // get dispatch size
        constexpr auto dim = luisa::vector_dimension_v<Size>;
        uint3 dispatch_size_uint3{1u, 1u, 1u};
        for (auto i = 0u; i < dim; i++) {
            dispatch_size_uint3[i] = dispatch_size[i];
        }
        uint dispatch_count = dispatch_size_uint3.x * dispatch_size_uint3.y * dispatch_size_uint3.z;
        uint duplicate_count = 1u;

        // init
        _aggregated_kernel_queue.clear_counter_buffer(command_buffer);
        auto launch_state_count = duplicate_count * dispatch_count;
        while (launch_state_count > 0 || _queue_empty) {
            _queue_empty = true;

        }
    }
};

}// namespace luisa::compute