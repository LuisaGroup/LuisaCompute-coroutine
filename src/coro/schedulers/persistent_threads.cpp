//
// Created by Mike on 2024/5/10.
//

#include <luisa/dsl/sugar.h>
#include <luisa/coro/coro_graph.h>
#include <luisa/coro/coro_frame_smem.h>
#include <luisa/coro/coro_frame_buffer.h>
#include <luisa/coro/schedulers/persistent_threads.h>

namespace luisa::compute::coroutine::detail {

void persistent_threads_coro_scheduler_main_kernel_impl(
    const PersistentThreadsCoroSchedulerConfig &config,
    uint q_fac, uint g_fac, uint shared_queue_size, uint global_queue_size,
    const CoroGraph *graph, Shared<CoroFrame> &frames, Expr<uint3> dispatch_shape,
    Expr<Buffer<uint>> global, const Buffer<CoroFrame> &global_frames,
    luisa::move_only_function<void(CoroFrame &, CoroToken)> call_subroutine) noexcept {

    auto subroutine_count = static_cast<uint>(graph->nodes().size());
    Shared<uint> path_id{shared_queue_size};
    Shared<uint> work_counter{subroutine_count};
    Shared<uint> work_offset{2u};
    Shared<uint> all_token{config.global_memory_ext ?
                               shared_queue_size + global_queue_size :
                               shared_queue_size};
    Shared<uint> workload{2};
    Shared<uint> work_stat{2};// work_state[0] for max_count, [1] for max_id
    for (auto index : dsl::dynamic_range(q_fac)) {
        auto s = index * config.block_size + thread_x();
        all_token[s] = 0u;
        // frames.write(s, coro.instantiate(), std::array{0u, 1u});
    }
    for (auto index : dsl::dynamic_range(g_fac)) {
        auto s = index * config.block_size + thread_x();
        all_token[shared_queue_size + s] = 0u;
    }
    $if (thread_x() < subroutine_count) {
        $if (thread_x() == 0u) {
            work_counter[thread_x()] =
                config.global_memory_ext ?
                    shared_queue_size + global_queue_size :
                    shared_queue_size;
        }
        $else {
            work_counter[thread_x()] = 0u;
        };
    };
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
    $while ((rem_global[0] != 0u | rem_local[0] != 0u) & (count != count_limit)) {
        sync_block();//very important, synchronize for condition
        rem_local[0] = 0u;
        count += 1;
        work_stat[0] = 0;
        work_stat[1] = -1;
        sync_block();
        $if (thread_x() == config.block_size - 1) {
            $if (workload[0] >= workload[1] & rem_global[0] == 1u) {//fetch new workload
                workload[0] = global.atomic(0u).fetch_add(config.block_size * config.fetch_size);
                workload[1] = min(workload[0] + config.block_size * config.fetch_size, dispatch_size);
                $if (workload[0] >= dispatch_size) {
                    rem_global[0] = 0u;
                };
            };
        };
        sync_block();
        $if (thread_x() < subroutine_count) {//get max
            $if (workload[0] < workload[1] | thread_x() != 0u) {
                $if (work_counter[thread_x()] != 0) {
                    rem_local[0] = 1u;
                    work_stat.atomic(0).fetch_max(work_counter[thread_x()]);
                };
            };
        };
        sync_block();
        $if (thread_x() < subroutine_count) {//get argmax
            $if (work_stat[0] == work_counter[thread_x()] & (workload[0] < workload[1] | thread_x() != 0u)) {
                work_stat[1] = thread_x();
            };
        };
        sync_block();
        work_offset[0] = 0;
        work_offset[1] = 0;
        sync_block();
        if (!config.global_memory_ext) {
            for (auto index : dsl::dynamic_range(q_fac)) {//collect indices
                auto frame_token = all_token[index * config.block_size + thread_x()];
                $if (frame_token == work_stat[1]) {
                    auto id = work_offset.atomic(0).fetch_add(1u);
                    path_id[id] = index * config.block_size + thread_x();
                };
            }
        } else {
            for (auto index : dsl::dynamic_range(q_fac)) {//collect switch out indices
                auto frame_token = all_token[index * config.block_size + thread_x()];
                $if (frame_token != work_stat[1]) {
                    auto id = work_offset.atomic(0).fetch_add(1u);
                    path_id[id] = index * config.block_size + thread_x();
                };
            }
            sync_block();
            $if (shared_queue_size - work_offset[0] < config.block_size) {//no enough work
                for (auto index : dsl::dynamic_range(g_fac)) {            //swap frames
                    auto global_id = block_x() * global_queue_size + index * config.block_size + thread_x();
                    auto g_queue_id = index * config.block_size + thread_x();
                    auto coro_token = all_token[shared_queue_size + g_queue_id];
                    $if (coro_token == work_stat[1]) {
                        auto id = work_offset.atomic(1).fetch_add(1u);
                        $if (id < work_offset[0]) {
                            auto dst = path_id[id];
                            auto frame_token = all_token[dst];
                            $if (coro_token != 0u) {
                                $if (frame_token != 0u) {
                                    auto g_state = global_frames->read(global_id);
                                    global_frames->write(global_id, frames.read(dst));
                                    frames.write(dst, g_state);
                                    all_token[shared_queue_size + g_queue_id] = frame_token;
                                    all_token[dst] = coro_token;
                                }
                                $else {
                                    auto g_state = global_frames->read(global_id);
                                    frames.write(dst, g_state);
                                    all_token[shared_queue_size + g_queue_id] = frame_token;
                                    all_token[dst] = coro_token;
                                };
                            }
                            $else {
                                $if (frame_token != 0u) {
                                    auto frame = frames.read(dst);
                                    global_frames->write(global_id, frame);
                                    all_token[shared_queue_size + g_queue_id] = frame_token;
                                    all_token[dst] = coro_token;
                                };
                            };
                        };
                    };
                }
            };
        }
        auto gen_st = workload[0];
        sync_block();
        auto pid = def(0u);
        if (config.global_memory_ext) {
            pid = thread_x();
        } else {
            pid = path_id[thread_x()];
        }
        auto launch_condition = def(true);
        if (!config.global_memory_ext) {
            launch_condition = (thread_x() < work_offset[0]);
        } else {
            launch_condition = (all_token[pid] == work_stat[1]);
        }
        $if (launch_condition) {
            $switch (all_token[pid]) {
                $case (0u) {
                    $if (gen_st + thread_x() < workload[1]) {
                        work_counter.atomic(0u).fetch_sub(1u);
                        auto global_index = gen_st + thread_x();
                        auto image_size = dispatch_shape.x * dispatch_shape.y;
                        auto index_z = global_index / image_size;
                        auto index_xy = global_index % image_size;
                        auto index_x = index_xy % dispatch_shape.x;
                        auto index_y = index_xy / dispatch_shape.x;
                        auto frame = CoroFrame::create(graph->shared_frame(), make_uint3(index_x, index_y, index_z));
                        call_subroutine(frame, coro_token_entry);
                        auto next = frame.target_token & coro_token_valid_mask;
                        frames.write(pid, frame, graph->entry().output_fields());
                        all_token[pid] = next;
                        work_counter.atomic(next).fetch_add(1u);
                        workload.atomic(0).fetch_add(1u);
                    };
                };
                for (auto i = 1u; i < subroutine_count; i++) {
                    $case (i) {
                        work_counter.atomic(i).fetch_sub(1u);
                        auto frame = frames.read(pid, graph->node(i).input_fields());
                        call_subroutine(frame, i);
                        auto next = frame.target_token & coro_token_valid_mask;
                        frames.write(pid, frame, graph->node(i).output_fields());
                        all_token[pid] = next;
                        work_counter.atomic(next).fetch_add(1u);
                    };
                }
            };
        };
        sync_block();
    };
#ifndef NDEBUG
    $if (count >= count_limit) {
        device_log("block_id{},thread_id {}, loop not break! local:{}, global:{}", block_x(), thread_x(), rem_local[0], rem_global[0]);
        $if (thread_x() < subroutine_count) {
            device_log("work rem: id {}, size {}", thread_x(), work_counter[thread_x()]);
        };
    };
#endif
}

}// namespace luisa::compute::coroutine::detail
