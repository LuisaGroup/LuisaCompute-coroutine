//
// Created by Mike on 2024/5/10.
//

#pragma once

#include <luisa/coro/v2/coro_scheduler.h>
#include <luisa/coro/v2/coro_func.h>
#include <luisa/coro/v2/coro_graph.h>
#include <luisa/coro/v2/coro_frame_soa.h>
#include <luisa/coro/v2/coro_frame_buffer.h>

namespace luisa::compute::coroutine {

struct WavefrontCoroSchedulerConfig {
    uint3 block_size = luisa::make_uint3(128, 1, 1);
    uint max_instance_count = 2_M;
    bool soa = true;
    bool sort = true;// use sort for coro token gathering
    bool compact = true;
    bool debug = false;
    uint hint_range = 0xffff'ffff;
    luisa::vector<luisa::string> hint_fields;
};

template<typename... Args>
class WavefrontCoroScheduler : public CoroScheduler<Args...> {

private:
    Shader1D<Buffer<uint>, Buffer<uint>, uint, uint, uint, Args...> _gen_shader;
    luisa::vector<Shader1D<Buffer<uint>, Buffer<uint>, uint, Args...>> _resume_shaders;
    Shader1D<Buffer<uint>, Buffer<uint>, uint> _count_prefix_shader;
    Shader1D<Buffer<uint>, Buffer<uint>, uint> _gather_shader;
    Shader1D<Buffer<uint>, uint> _initialize_shader;
    Shader1D<Buffer<uint>, uint, uint> _compact_shader;
    Shader1D<Buffer<uint>, uint> _clear_shader;
    SOA<CoroFrame> _frame_soa;
    Buffer<CoroFrame> _frame_buffer;
    Buffer<uint> _resume_index;
    Buffer<uint> _resume_count;
    ///offset calculate from count, will be end after gathering
    Buffer<uint> _resume_offset;
    Buffer<uint> _global_buffer;
    luisa::vector<uint> _host_count;
    luisa::vector<uint> _host_offset;
    bool _host_empty;
    uint _dispatch_counter;
    uint _max_sub_coro;
    uint _max_frame_count;
    radix_sort::temp_storage _sort_temp_storage;
    radix_sort::instance<> _sort_token;
    radix_sort::instance<Buffer<uint>> _sort_hint;
    luisa::vector<bool> _have_hint;
    Buffer<uint> _temp_key[2];
    Buffer<uint> _temp_index;

private:
    void _create_shader(Device &device, const Coroutine<void(Args...)> &coroutine,
                        const WavefrontCoroSchedulerConfig &config) noexcept {
        luisa::shared_ptr<CoroFrameDesc> desc = coroutine.shared_frame();
        if (config.soa) {
            _frame_soa = device.create_coro_frame_soa(coroutine.shared_frame(), config.max_instance_count);
        } else {
            _frame_buffer = device.create_coro_frame_buffer(coroutine.shared_frame(), config.max_instance_count);
        }
        bool use_sort = config.sort || !config.hint_fields.empty();
        _max_sub_coro = coroutine->suspend_count() + 1;
        _resume_index = device.create_buffer<uint>(_max_frame_count);
        if (use_sort) {
            _temp_index = device.create_buffer<uint>(_max_frame_count);
            _temp_key[0] = device.create_buffer<uint>(_max_frame_count);
            _temp_key[1] = device.create_buffer<uint>(_max_frame_count);
        }
        _resume_count = device.create_buffer<uint>(_max_sub_coro);
        _resume_offset = device.create_buffer<uint>(_max_sub_coro);
        _global_buffer = device.create_buffer<uint>(1);
        _host_empty = true;
        _dispatch_counter = 0;
        _host_offset.resize(_max_sub_coro);
        _host_count.resize(_max_sub_coro);
        _have_hint.resize(_max_sub_coro, false);
        for (auto &token : config.hint_fields) {
            auto id = coroutine->coro_tokens().find(token);
            if (id != coroutine->coro_tokens().end()) {
                LUISA_ASSERT(id->second < _max_sub_coro,
                             "coroutine token {} of id {} out of range {}", token, id->second, _max_sub_coro);
                _have_hint[id->second] = true;
            } else {
                LUISA_WARNING("coroutine token {} not found, hint disabled", token);
            }
        }
        for (auto i = 0u; i < _max_sub_coro; i++) {
            if (i) {
                _host_count[i] = 0;
                _host_offset[i] = _max_frame_count;
            } else {
                _host_count[i] = _max_frame_count;
                _host_offset[i] = 0;
            }
        }
        Callable get_coro_token = [&](UInt index) {
            $if (index > _max_frame_count) {
                device_log("index {} out of range {}", index, _max_frame_count);
            };
            if (config.soa) {
                return _frame_soa->read_field<uint>(index, luisa::string_view("target_token")) & token_mask;
            } else {
                CoroFrame frame = _frame_buffer->read(index);
                return frame.get<uint>("target_token") & token_mask;
            }
        };
        Callable identical = [](UInt index) {
            return index;
        };

        Callable keep_index = [](UInt index, BufferUInt val) {
            return val.read(index);
        };
        Callable get_coro_hint = [&](UInt index, BufferUInt val) {
            if (!config.hint_fields.empty()) {
                auto id = keep_index(index, val);
                CoroFrame frame = CoroFrame::create(desc);
                if (config.soa) {
                    frame = _frame_soa->read(id, std::array{desc->designated_field("coro_hint")});
                } else {
                    frame = _frame_buffer->read(id);
                }
                return frame.get<uint>("coro_hint");
            }
            return def<uint>(0u);
        };
        if (use_sort) {
            _sort_temp_storage = radix_sort::temp_storage(
                device, _max_frame_count, std::max(std::min(config.hint_range, 128u), _max_sub_coro));
        }
        if (config.sort) {
            _sort_token = radix_sort::instance<>(
                device, _max_frame_count, _sort_temp_storage, &get_coro_token, &identical,
                &get_coro_token, 1, _max_sub_coro);
        }
        if (!config.hint_fields.empty()) {
            if (config.hint_range <= 128) {
                _sort_hint = radix_sort::instance<Buffer<uint>>(
                    device, _max_frame_count, _sort_temp_storage, &get_coro_hint, &keep_index,
                    &get_coro_hint, 1, config.hint_range);
            } else {
                auto highbit = 0;
                while ((config.hint_range >> highbit) != 1) {
                    highbit++;
                }
                _sort_hint = radix_sort::instance<Buffer<uint>>(
                    device, _max_frame_count, _sort_temp_storage, &get_coro_hint, &keep_index,
                    &get_coro_hint, 0, 128, 0, highbit);
            }
        }
        Kernel1D gen_kernel = [&](BufferUInt index, BufferUInt count, UInt offset, UInt st_task_id, UInt n, Var<Args>... args) {
            auto x = dispatch_x();
            $if (x >= n) {
                $return();
            };
            UInt frame_id;
            if (!config.compact) {
                frame_id = index->read(x);
            } else {
                frame_id = offset + x;
            }
            CoroFrame frame = CoroFrame::create(desc, def<uint3>(st_task_id + x, 0, 0));
            if (!config.sort) {
                count.atomic(0u).fetch_add(-1u);
            }

            coroutine.subroutine(0u)(frame, args...);
            if (config.soa) {
                _frame_soa->write(frame_id, frame, coroutine->graph().node(0u)->output_state_members);
            } else {
                _frame_buffer->write(frame_id, frame);
            }
            if (!config.sort) {
                auto nxt = read_promise<uint>(frame, "coro_token") & token_mask;
                count.atomic(nxt).fetch_add(1u);
            }
        };
    }

    void _dispatch(Stream &stream, uint3 dispatch_size,
                   compute::detail::prototype_to_shader_invocation_t<Args>... args) noexcept override {
        LUISA_ERROR_WITH_LOCATION("Unimplemented");
    }

public:
    WavefrontCoroScheduler(Device &device, const Coroutine<void(Args...)> &coro,
                           const WavefrontCoroSchedulerConfig &config) noexcept {
        _create_shader(device, coro, config);
    }
    WavefrontCoroScheduler(Device &device, const Coroutine<void(Args...)> &coro) noexcept
        : WavefrontCoroScheduler{device, coro, WavefrontCoroSchedulerConfig{}} {}
};

template<typename... Args>
WavefrontCoroScheduler(Device &, const Coroutine<void(Args...)> &)
    -> WavefrontCoroScheduler<Args...>;

template<typename... Args>
WavefrontCoroScheduler(Device &, const Coroutine<void(Args...)> &,
                       const WavefrontCoroSchedulerConfig &)
    -> WavefrontCoroScheduler<Args...>;

}// namespace luisa::compute::coroutine