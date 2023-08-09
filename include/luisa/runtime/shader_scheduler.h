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

    [[nodiscard]] luisa::unique_ptr<ShaderDispatchCommand> dispatch() noexcept;
    void encode_uniform(const void *data, size_t size) noexcept;
    void encode_buffer(uint64_t handle, size_t offset, size_t size) noexcept;
    void encode_texture(uint64_t handle, uint32_t level) noexcept;
    void encode_bindless_array(uint64_t handle) noexcept;
    void encode_accel(uint64_t handle) noexcept;
};

class LC_RUNTIME_API ShaderFrame {

public:
    uint frame_handle;

};

class LC_RUNTIME_API ShaderFrameManager {

public:
    luisa::vector<ShaderFrame> frames;
    luisa::unordered_map<uint, luisa::set<uint>> shader_frame_map;
};

#define INVALID_KERNEL 0u

class LC_RUNTIME_API ShaderScheduler {

private:
    AggregatedRayQueue _aggregated_kernel_queue;
    luisa::unique_ptr<luisa::function<void(uint)>> _launch_kernel;
    luisa::unordered_map<uint, KernelInfo> _kernel_info;
    Device &_device;

    uint _state_limit;
//    bool _gathering;
//    bool _direct_launch;
//    bool _use_tag_sort;
    Buffer<uint> _kernel_indexes;
    ShaderFrameManager _frame_manager;

public:
    ShaderScheduler(Device &device, size_t state_count, uint kernel_count, bool gathering, uint state_limit) noexcept
        : _device{device}, _aggregated_kernel_queue{device, state_count, kernel_count, gathering},
//          _gathering{gathering}, _direct_launch{false}, _use_tag_sort{false}
          _state_limit{state_limit} {}

    void emplace_kernel(KernelInfo kernel_info) noexcept {
        _kernel_info.try_emplace(_kernel_info.size() + 1u, std::move(kernel_info));
    }

    template<typename Size>
        requires luisa::is_uint_vector_v<Size>
    void dispatch(CommandBuffer &command_buffer, Size dispatch_size) noexcept {
        // get dispatch size
        constexpr auto dim = luisa::vector_dimension_v<Size>;
        uint3 dispatch_size_uint3{1u, 1u, 1u};
        for (auto i = 0u; i < dim; i++) {
            dispatch_size_uint3[i] = dispatch_size[i];
        }
        uint dispatch_count = dispatch_size_uint3.x * dispatch_size_uint3.y * dispatch_size_uint3.z;

        // get_config
        // TODO
        uint duplicate_count = 1u;
        uint state_count = _state_limit;

        // shader compile
        LUISA_INFO("Compiling management kernels.");
        auto mark_invalid_shader = _device.compile_async<1>([&](BufferUInt invalid_queue, BufferUInt invalid_queue_size) noexcept {
            auto dispatch_id = dispatch_x();
//            if (_gathering) {
//                invalid_queue.write(dispatch_id, dispatch_id);
//            }
            invalid_queue_size.write(0u, state_count);
//            if (_gathering) {
//                _kernel_indexes->write(dispatch_id, INVALID_KERNEL);
//            }
        });

//        auto gather_shader = _device.compile_async<1>([&](BufferUInt queue, BufferUInt queue_size, UInt kernel_id, UInt n) noexcept {
//            if (_gathering) {
//                auto path_id = dispatch_x();
//                auto kernel = def(0u);
//                $if(dispatch_x() < n) {
//                    kernel = _kernel_indexes->read(path_id);
//                };
//                /*$if(kernel == kernel_id) {
//                    auto slot = queue_size.atomic(0u).fetch_add(1u);
//                                    queue.write(slot, path_id);
//                };*/
//
//                auto slot = def(0u);
//                {
//                    Shared<uint> index{1u};
//                    $if(thread_x() == 0u) { index.write(0u, 0u); };
//                    sync_block();
//                    auto local_index = def(0u);
//                    $if(dispatch_x() < n & kernel == kernel_id) {
//                        local_index = index.atomic(0u).fetch_add(1u);
//                    };
//                    sync_block();
//                    $if(thread_x() == 0u) {
//                        auto local_count = index.read(0u);
//                        auto global_offset = queue_size.atomic(0u).fetch_add(local_count);
//                        index.write(0u, global_offset);
//                    };
//                    sync_block();
//                    slot = index.read(0u) + local_index;
//                }
//                $if(dispatch_x() < n & kernel == kernel_id) {
//                    queue.write(slot, path_id);
//                };
//            }
//        });
//
//        auto sort_tag_gather_shader = _device.compile_async<1>([&](BufferUInt queue, BufferUInt tags, BufferUInt tag_counter,
//                                                                   UInt kernel_id, UInt tag_size) noexcept {
//            if (_gathering && _use_tag_sort) {
//                auto path_id = dispatch_x();
//                $if(path_id < state_count) {
//                    auto kernel = _kernel_indexes->read(path_id);
//                    auto tag = tags.read(path_id);
//                    $if(kernel == kernel_id) {
//                        //                        if (pipeline().surfaces().size() <= 32) {//not sure what is the proper threshold
//                        //
//                        //                            for (auto i = 0u; i < pipeline().surfaces().size(); ++i) {
//                        //                                $if(tag == i) {
//                        //                                    auto queue_id = tag_counter.atomic(i).fetch_add(1u);
//                        //                                    queue.write(queue_id, path_id);
//                        //                                };
//                        //                            }
//                        //                        } else {
//                        auto queue_id = tag_counter.atomic(tag).fetch_add(1u);
//                        queue.write(queue_id, path_id);
//                        //                        }
//                    };
//                };
//            }
//        });
//
//        auto bucket_update_shader = _device.compile_async<1>([&](BufferUInt tag_counter) noexcept {
//            if (_use_tag_sort) {
//                tag_counter.write(dispatch_x(), 0u);
//            }
//        });
//
//        auto bucket_reset_shader = _device.compile_async<1>([&](BufferUInt tag_counter, UInt tag_size) noexcept {
//            if (_use_tag_sort) {
//                $for(i, 0u, tag_size) {
//                    tag_counter.write(i, 0u);
//                };
//            }
//        });

        // init
        _aggregated_kernel_queue.clear_counter_buffer(command_buffer);
        auto launch_state_count = duplicate_count * dispatch_count;
        auto last_committed_state = launch_state_count;
        auto queue_empty = true;
        command_buffer << mark_invalid_shader.get()(_aggregated_kernel_queue.index_buffer(INVALID_KERNEL),
                                                    _aggregated_kernel_queue.counter_buffer(INVALID_KERNEL))
                              .dispatch(state_count);

        while (launch_state_count > 0 || queue_empty) {
            queue_empty = true;
            _aggregated_kernel_queue.catch_counter(command_buffer);

            auto invalid_count = _aggregated_kernel_queue.host_counter(INVALID_KERNEL);
            if (invalid_count > state_count / 2 && launch_state_count > 0) {
                // launch new kernel

                auto generate_count = std::min(launch_state_count, invalid_count);
                auto zero = 0u;
                auto valid_count = state_count - invalid_count;

                _aggregated_kernel_queue.clear_counter_buffer(command_buffer, INVALID_KERNEL);

                // TODO: compact & sort
                // ...


//                command_buffer << synchronize();
                command_buffer << _kernel_info[1u].dispatch();  // separated kernels
                launch_state_count -= generate_count;
                queue_empty = false;
                continue;
            }

            auto setup_workload = [&](uint max_index) {
                _aggregated_kernel_queue.clear_counter_buffer(command_buffer, max_index);
            };
            auto launch_kernel = [&](uint dispatch_index) {
                auto dispatch_size = _aggregated_kernel_queue.host_counter(dispatch_index);

                // TODO: dispatch different shaders by state tags
                // get launch kernel from IR
                for (auto iter = _kernel_info.begin(); iter != _kernel_info.end(); ++iter) {
                    if (iter->first == dispatch_index) {
                        command_buffer << iter->second.dispatch();
                        break;
                    }
                }
                LUISA_ERROR_WITH_LOCATION("Unexpected dispatch index: ", dispatch_index);
            };
        }
    }
};

}// namespace luisa::compute