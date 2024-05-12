//
// Created by Mike on 2024/5/10.
//

#pragma once

#include <luisa/coro/v2/coro_func.h>
#include <luisa/coro/v2/coro_graph.h>
#include <luisa/coro/v2/coro_scheduler.h>

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
    // Shader1D<Buffer<uint>, Buffer<uint>, uint, Container, uint, uint, Args...> _gen_shader;
    // luisa::vector<Shader1D<Buffer<uint>, Buffer<uint>, Container, uint, Args...>> _resume_shaders;
    // Shader1D<Buffer<uint>, Buffer<uint>, uint> _count_prefix_shader;
    // Shader1D<Buffer<uint>, Buffer<uint>, Container, uint> _gather_shader;
    // Shader1D<Buffer<uint>, Container, uint> _initialize_shader;
    // Shader1D<Buffer<uint>, Container, uint, uint> _compact_shader;
    // Shader1D<Buffer<uint>, uint> _clear_shader;
    // compute::Buffer<uint> _resume_index;
    // compute::Buffer<uint> _resume_count;
    // ///offset calculate from count, will be end after gathering
    // compute::Buffer<uint> _resume_offset;
    // compute::Buffer<uint> _global_buffer;
    // compute::Buffer<uint> _debug_buffer;
    // luisa::vector<uint> _host_count;
    // luisa::vector<uint> _host_offset;
    // bool _host_empty;
    // uint _dispatch_counter;
    // uint _max_sub_coro;
    // uint _max_frame_count;
    // radix_sort::temp_storage _sort_temp_storage;
    // radix_sort::instance<> _sort_token;
    // radix_sort::instance<Buffer<uint>> _sort_hint;
    // luisa::vector<bool> _have_hint;
    // compute::Buffer<uint> _temp_key[2];
    // compute::Buffer<uint> _temp_index;

private:
    void _create_shader(Device &device, const Coroutine<void(Args...)> &coro,
                        const WavefrontCoroSchedulerConfig &config) noexcept {
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