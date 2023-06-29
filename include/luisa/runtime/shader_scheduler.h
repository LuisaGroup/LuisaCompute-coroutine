//
// Created by ChenXin on 2023/6/29.
//

#pragma once

#include <luisa/runtime/shader.h>

#include <luisa/dsl/syntax.h>
#include <luisa/dsl/sugar.h>
#include <luisa/dsl/builtin.h>
#include <luisa/runtime/command_buffer.h>

namespace luisa::compute {

class RayQueue {

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

class AggregatedRayQueue {
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

class KernelInfo {
private:
    uint _kernel_state;
    luisa::unique_ptr<Resource> _shader;
    uint _shader_type;
    uint _dim;
    uint _arg_count;
    uint _uniform_count;
    std::vector<uint> _arg_handles;

public:
    [[nodiscard]] auto dispatch() const noexcept;

public:
    [[nodiscard]] auto kernel_state() const noexcept { return _kernel_state; }
};

class ShaderScheduler {
private:
    AggregatedRayQueue _aggregated_kernel_queue;
    luisa::unique_ptr<luisa::function<void(uint)>> _launch_kernel;
    luisa::unordered_map<int, KernelInfo> _kernel_info;
    Device &_device;
    bool _gathering;

public:
    ShaderScheduler(Device &device, size_t state_count, uint kernel_count, bool gathering) noexcept
        : _device{device}, _aggregated_kernel_queue{device, state_count, kernel_count, gathering},
          _gathering{gathering} {}
};

}// namespace luisa::compute