//
// Created by Mike on 2024/5/10.
//

#pragma once

#include <luisa/coro/v2/coro_scheduler.h>

namespace luisa::compute::coroutine {

struct PersistentThreadsCoroSchedulerConfig {
    uint thread_count = 64_k;
    uint block_size = 128;
    uint fetch_size = 16;
    bool shared_memory_soa = true;
    bool global_ext_memory = false;
};

template<typename... Args>
class PersistentThreadsCoroScheduler : public CoroScheduler<Args...> {

public:
    using Coro = Coroutine<void(Args...)>;
    using Config = PersistentThreadsCoroSchedulerConfig;

private:
    Config _config;
    Shader1D<Buffer<uint>, uint3, Args...> _pt_shader;
    Shader1D<Buffer<uint>> _clear_shader;
    Buffer<uint> _global;
    Buffer<CoroFrame> _global_frames;
    Shader1D<uint> _initialize_shader;

private:
    void _prepare(Device &device, const Coro &coro) noexcept {
        _global = device.create_buffer<uint>(1);
        auto q_fac = 1u;
        auto g_fac = coro.subroutine_count() - q_fac;
        auto global_queue_size = _config.block_size * g_fac;
        if (_config.global_ext_memory) {
            auto global_ext_size = _config.thread_count * g_fac;
            _global_frames = device.create_buffer<CoroFrame>(coro.shared_frame(), global_ext_size);
        }
        Kernel1D main_kernel = [&](BufferUInt global, UInt3 dispatch_shape, Var<Args>... args) noexcept {
            set_block_size(_config.block_size, 1u, 1u);
            auto shared_queue_size = _config.block_size * q_fac;
            Shared<CoroFrame> frames{coro.shared_frame(), shared_queue_size, _config.shared_memory_soa};
            Shared<uint> path_id{shared_queue_size};
            Shared<uint> work_counter{coro.subroutine_count()};
            Shared<uint> work_offset{2u};
            Shared<uint> all_token{_config.global_ext_memory ?
                                       shared_queue_size + global_queue_size :
                                       shared_queue_size};
            Shared<uint> workload{2};
            Shared<uint> work_stat{2};// work_state[0] for max_count, [1] for max_id
            for (auto index : dsl::dynamic_range(q_fac)) {
                auto s = index * _config.block_size + thread_x();
                all_token[s] = 0u;
                // frames.write(s, coro.instantiate(), std::array{0u, 1u});
            }
            for (auto index : dsl::dynamic_range(g_fac)) {
                auto s = index * _config.block_size + thread_x();
                all_token[shared_queue_size + s] = 0u;
            }
            if_(thread_x() < coro.subroutine_count(), [&] {
                if_(thread_x() == 0u, [&] {
                    work_counter[thread_x()] =
                        _config.global_ext_memory ?
                            shared_queue_size + global_queue_size :
                            shared_queue_size;
                }).else_([&] {
                    work_counter[thread_x()] = 0u;
                });
            });
            workload[0] = 0u;
            workload[1] = 0u;
            Shared<uint> rem_global{1};
            Shared<uint> rem_local{1};
            rem_global[0] = 1u;
            rem_local[0] = 0u;
            sync_block();
            auto count = def(0u);
            auto count_limit = def<uint>(-1);
            auto dispatch_size = dispatch_shape.x * dispatch_shape.y * dispatch_shape.z;
            loop([&] {
                if_(!((rem_global[0] != 0u | rem_local[0] != 0u) & (count != count_limit)), [&] { break_(); });
                sync_block();//very important, synchronize for condition
                rem_local[0] = 0u;
                count += 1;
                work_stat[0] = 0;
                work_stat[1] = -1;
                sync_block();
                if_(thread_x() == _config.block_size - 1, [&] {
                    if_(workload[0] >= workload[1] & rem_global[0] == 1u, [&] {//fetch new workload
                        workload[0] = global.atomic(0u).fetch_add(_config.block_size * _config.fetch_size);
                        workload[1] = min(workload[0] + _config.block_size * _config.fetch_size, dispatch_size);
                        if_(workload[0] >= dispatch_size, [&] {
                            rem_global[0] = 0u;
                        });
                    });
                });
                sync_block();
                if_(thread_x() < coro.subroutine_count(), [&] {//get max
                    if_(workload[0] < workload[1] | thread_x() != 0u, [&] {
                        if_(work_counter[thread_x()] != 0, [&] {
                            rem_local[0] = 1u;
                            work_stat.atomic(0).fetch_max(work_counter[thread_x()]);
                        });
                    });
                });
                sync_block();
                if_(thread_x() < coro.subroutine_count(), [&] {//get argmax
                    if_(work_stat[0] == work_counter[thread_x()] & (workload[0] < workload[1] | thread_x() != 0u), [&] {
                        work_stat[1] = thread_x();
                    });
                });
                sync_block();
                work_offset[0] = 0;
                work_offset[1] = 0;
                sync_block();
                if (!_config.global_ext_memory) {
                    for (auto index : dsl::dynamic_range(q_fac)) {//collect indices
                        auto frame_token = all_token[index * _config.block_size + thread_x()];
                        if_(frame_token == work_stat[1], [&] {
                            auto id = work_offset.atomic(0).fetch_add(1u);
                            path_id[id] = index * _config.block_size + thread_x();
                        });
                    }
                } else {
                    for (auto index : dsl::dynamic_range(q_fac)) {//collect switch out indices
                        auto frame_token = all_token[index * _config.block_size + thread_x()];
                        if_(frame_token != work_stat[1], [&] {
                            auto id = work_offset.atomic(0).fetch_add(1u);
                            path_id[id] = index * _config.block_size + thread_x();
                        });
                    }
                    sync_block();
                    if_(shared_queue_size - work_offset[0] < _config.block_size, [&] {//no enough work
                        for (auto index : dsl::dynamic_range(g_fac)) {                //swap frames
                            auto global_id = block_x() * global_queue_size + index * _config.block_size + thread_x();
                            auto g_queue_id = index * _config.block_size + thread_x();
                            auto coro_token = all_token[shared_queue_size + g_queue_id];
                            if_(coro_token == work_stat[1], [&] {
                                auto id = work_offset.atomic(1).fetch_add(1u);
                                if_(id < work_offset[0], [&] {
                                    auto dst = path_id[id];
                                    auto frame_token = all_token[dst];
                                    if_(coro_token != 0u, [&] {
                                        $if (frame_token != 0u) {
                                            auto g_state = _global_frames->read(global_id);
                                            _global_frames->write(global_id, frames.read(dst));
                                            frames.write(dst, g_state);
                                            all_token[shared_queue_size + g_queue_id] = frame_token;
                                            all_token[dst] = coro_token;
                                        }
                                        $else {
                                            auto g_state = _global_frames->read(global_id);
                                            frames.write(dst, g_state);
                                            all_token[shared_queue_size + g_queue_id] = frame_token;
                                            all_token[dst] = coro_token;
                                        };
                                    }).else_([&] {
                                        $if (frame_token != 0u) {
                                            auto frame = frames.read(dst);
                                            _global_frames->write(global_id, frame);
                                            all_token[shared_queue_size + g_queue_id] = frame_token;
                                            all_token[dst] = coro_token;
                                        };
                                    });
                                });
                            });
                        }
                    });
                }
                auto gen_st = workload[0];
                sync_block();
                auto pid = def(0u);
                if (_config.global_ext_memory) {
                    pid = thread_x();
                } else {
                    pid = path_id[thread_x()];
                }
                auto launch_condition = def(true);
                if (!_config.global_ext_memory) {
                    launch_condition = (thread_x() < work_offset[0]);
                } else {
                    launch_condition = (all_token[pid] == work_stat[1]);
                }
                if_(launch_condition, [&] {
                    auto switch_stmt = switch_(all_token[pid]);
                    std::move(switch_stmt).case_(0u, [&] {
                        if_(gen_st + thread_x() < workload[1], [&] {
                            work_counter.atomic(0u).fetch_sub(1u);
                            auto global_index = gen_st + thread_x();
                            auto image_size = dispatch_shape.x * dispatch_shape.y;
                            auto index_z = global_index / image_size;
                            auto index_xy = global_index % image_size;
                            auto index_x = index_xy % dispatch_shape.x;
                            auto index_y = index_xy / dispatch_shape.x;
                            auto frame = coro.instantiate(make_uint3(index_x, index_y, index_z));
                            coro.entry()(frame, args...);
                            auto next = frame.target_token & token_mask;
                            frames.write(pid, frame, coro.graph()->entry().output_fields());
                            all_token[pid] = next;
                            work_counter.atomic(next).fetch_add(1u);
                            workload.atomic(0).fetch_add(1u);
                        });
                    });
                    for (auto i = 1u; i < coro.subroutine_count(); i++) {
                        std::move(switch_stmt).case_(i, [&] {
                            work_counter.atomic(i).fetch_sub(1u);
                            auto frame = frames.read(pid, coro.graph()->node(i).input_fields());
                            coro[i](frame, args...);
                            auto next = frame.target_token & token_mask;
                            frames.write(pid, frame, coro.graph()->node(i).output_fields());
                            all_token[pid] = next;
                            work_counter.atomic(next).fetch_add(1u);
                        });
                    }
                });
                sync_block();
            });
#ifndef NDEBUG
            if_(count >= count_limit, [&] {
                device_log("block_id{},thread_id {}, loop not break! local:{}, global:{}", block_x(), thread_x(), rem_local[0], rem_global[0]);
                if_(thread_x() < coro.subroutine_count(), [&] {
                    device_log("work rem: id {}, size {}", thread_x(), work_counter[thread_x()]);
                });
            });
#endif
        };
        _pt_shader = device.compile(main_kernel);
        _clear_shader = device.compile<1>([](BufferUInt global) {
            global->write(dispatch_x(), 0u);
        });
        if (_config.global_ext_memory) {
            _initialize_shader = device.compile<1>([&](UInt n) noexcept {
                auto x = dispatch_x();
                $if (x < n) {
                    _global_frames->write(x, coro.instantiate());
                };
            });
        }
    }

    void _dispatch(Stream &stream, uint3 dispatch_size,
                   compute::detail::prototype_to_shader_invocation_t<Args>... args) noexcept override {
        stream << _clear_shader(_global).dispatch(1u);
        if (_config.global_ext_memory) {
            auto n = static_cast<uint>(_global_frames.size());
            stream << _initialize_shader(n).dispatch(n);
        }
        stream << _pt_shader(_global, dispatch_size, args...).dispatch(_config.thread_count);
    }

public:
    PersistentThreadsCoroScheduler(Device &device, const Coro &coro, const Config &config) noexcept
        : _config{config} {
        _config.thread_count = luisa::align(_config.thread_count, _config.block_size);
        _prepare(device, coro);
    }
    PersistentThreadsCoroScheduler(Device &device, const Coro &coro) noexcept
        : PersistentThreadsCoroScheduler{device, coro, Config{}} {}
};

// User-defined CTAD guides
template<typename... Args>
PersistentThreadsCoroScheduler(Device &device, const Coroutine<void(Args...)> &coro,
                               const PersistentThreadsCoroSchedulerConfig &config) noexcept
    -> PersistentThreadsCoroScheduler<Args...>;

template<typename... Args>
PersistentThreadsCoroScheduler(Device &device, const Coroutine<void(Args...)> &coro) noexcept
    -> PersistentThreadsCoroScheduler<Args...>;

}// namespace luisa::compute::coroutine
