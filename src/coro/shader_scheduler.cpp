//
// Created by ChenXin on 2023/6/29.
//

#include "luisa/coro/shader_scheduler.h"

namespace luisa::compute {

RayQueue::RayQueue(luisa::compute::Device &device, size_t size) noexcept
    : _index_buffer{device.create_buffer<uint>(size)},
      _counter_buffer{device.create_buffer<uint>(counter_buffer_size)},
      _current_counter{counter_buffer_size} {
    _clear_counters = device.compile<1>([this] {
        _counter_buffer->write(dispatch_x(), 0u);
    });
}

BufferView<uint> RayQueue::prepare_counter_buffer(CommandBuffer &command_buffer) noexcept {
    if (_current_counter == counter_buffer_size) {
        _current_counter = 0u;
        command_buffer << _clear_counters().dispatch(counter_buffer_size);
    }
    return _counter_buffer.view(_current_counter++, 1u);
}

BufferView<uint> RayQueue::prepare_index_buffer(CommandBuffer &command_buffer) noexcept {
    return _index_buffer;
}

AggregatedRayQueue::AggregatedRayQueue(Device &device, size_t state_count,
                                       uint kernel_count, bool gathering) noexcept
    : _index_buffer{device.create_buffer<uint>(gathering ? state_count : kernel_count * state_count)},
      _counter_buffer{device.create_buffer<uint>(kernel_count)},
      _kernel_count{kernel_count},
      _state_count{state_count},
      _gathering{gathering} {
    _host_counter.resize(kernel_count);
    _offsets.resize(kernel_count);
    _clear_counters = device.compile<1>([this] {
        _counter_buffer->write(dispatch_x(), 0u);
    });
}

void AggregatedRayQueue::clear_counter_buffer(CommandBuffer &command_buffer, int index) noexcept {
    //if (_current_counter == counter_buffer_size-1) {
    //   _current_counter = 0u;
    if (index == -1) {
        command_buffer << _clear_counters().dispatch(_kernel_count);
    } else {
        uint zero = 0u;
        command_buffer << counter_buffer(index).copy_from(&zero);
    }
    //} else
    //    _current_counter++;
}

BufferView<uint> AggregatedRayQueue::counter_buffer(luisa::uint index) noexcept {
    return _counter_buffer.view(index, 1);
}

BufferView<uint> AggregatedRayQueue::index_buffer(luisa::uint index) noexcept {
    if (_gathering)
        return _index_buffer.view(_offsets[index], _host_counter[index]);
    else
        return _index_buffer.view(index * _state_count, _state_count);
}

uint AggregatedRayQueue::host_counter(uint index) const noexcept {
    return _host_counter[index];
}

void AggregatedRayQueue::catch_counter(CommandBuffer &command_buffer) noexcept {
    command_buffer << _counter_buffer.view(0, _kernel_count).copy_to(_host_counter.data());
    command_buffer << synchronize();
    uint prev = 0u;
    for (auto i = 0u; i < _kernel_count; ++i) {
        uint now = _host_counter[i];
        _offsets[i] = prev;
        prev += now;
    }
}

luisa::unique_ptr<ShaderDispatchCommand> KernelInfo::dispatch() noexcept {
    // dispatch by type

    // ComputeDispatchCmdEncoder
    ComputeDispatchCmdEncoder encoder{_shader_handle, _args.size(), _uniform_size};
    for (const auto &arg : _args) {
        auto &arg_tag = arg.tag;
        switch (arg_tag) {
            case ArgumentInfo::Tag::BUFFER: {
                auto arg_data = arg.buffer;
                encoder.encode_buffer(arg_data.handle, arg_data.offset, arg_data.size);
                break;
            }
            case ArgumentInfo::Tag::TEXTURE: {
                auto arg_data = arg.texture;
                encoder.encode_texture(arg_data.handle, arg_data.level);
                break;
            }
            case ArgumentInfo::Tag::UNIFORM: {
                auto arg_data = arg.uniform;
                encoder.encode_uniform(arg_data.data, arg_data.size);
                break;
            }
            case ArgumentInfo::Tag::BINDLESS_ARRAY: {
                auto arg_data = arg.bindless_array;
                encoder.encode_bindless_array(arg_data.handle);
                break;
            }
            case ArgumentInfo::Tag::ACCEL: {
                auto arg_data = arg.accel;
                encoder.encode_accel(arg_data.handle);
                break;
            }
            default:
                LUISA_ERROR_WITH_LOCATION("Unsupported kernel argument type.");
        }
    }
    _args.clear();
    return std::move(encoder).build();

    // RasterDispatchCmdEncoder
    // TODO: only when calculating rasterization on dx backend. (unsupported yet)
}

void KernelInfo::encode_uniform(const void *data, size_t size) noexcept {
    _uniform_size += size;
    ArgumentInfo arg{
        .tag = ArgumentInfo::Tag::UNIFORM,
        .uniform = ArgumentInfo::Uniform{
            .data = data,
            .size = size}};
    _args.emplace_back(arg);
}

void KernelInfo::encode_buffer(uint64_t handle, size_t offset, size_t size) noexcept {
    ArgumentInfo arg{
        .tag = ArgumentInfo::Tag::BUFFER,
        .buffer = ArgumentInfo::Buffer{
            .handle = handle,
            .offset = offset,
            .size = size}};
    _args.emplace_back(arg);
}

void KernelInfo::encode_texture(uint64_t handle, uint32_t level) noexcept {
    ArgumentInfo arg{
        .tag = ArgumentInfo::Tag::TEXTURE,
        .texture = ArgumentInfo::Texture{
            .handle = handle,
            .level = level}};
    _args.emplace_back(arg);
}

void KernelInfo::encode_bindless_array(uint64_t handle) noexcept {
    ArgumentInfo arg{
        .tag = ArgumentInfo::Tag::BINDLESS_ARRAY,
        .bindless_array = ArgumentInfo::BindlessArray{
            .handle = handle}};
    _args.emplace_back(arg);
}

void KernelInfo::encode_accel(uint64_t handle) noexcept {
    ArgumentInfo arg{
        .tag = ArgumentInfo::Tag::ACCEL,
        .accel = ArgumentInfo::Accel{
            .handle = handle}};
    _args.emplace_back(arg);
}

}// namespace luisa::compute