#pragma once

#include <luisa/core/dll_export.h>
#include <luisa/runtime/stream_event.h>
#include <luisa/runtime/stream.h>
#include <luisa/runtime/rhi/command_encoder.h>
#include <luisa/core/stl.h>

namespace luisa::compute {

class Stream;
inline namespace coro {
template<typename T>
class arg_to_view {
    using type = T;
};
template<typename T>
class arg_to_view<Buffer<T>> {
    using type = BufferView<T>;
};

template<typename T>
class CoroAwait;
template<typename T>
class CoroDispatcherBase {
    static_assert(always_false_v<T>);
};
template<typename FrameRef, typename... Args>
class CoroDispatcherBase<void(FrameRef, Args...)> : public concepts::Noncopyable {
public:

    luisa::queue<luisa::unique_ptr<ShaderDispatchCommand>> _dispatcher;
protected:
    using FuncType = void(FrameRef, Args...);
    std::tuple<arg_to_view<Args>...> _args;
    Coroutine<FuncType> *_coro;
    virtual void _await_step(Stream &stream) noexcept = 0;
    virtual void _await_all(Stream &stream) noexcept;
    uint dispatch_size;
    ///helper function for calling a shader by omitting suffix coroutine args
    template<size_t dim, typename... T>
    detail::ShaderInvoke<dim> call_shader(Shader<dim, T..., Args...> shader, T &&...prefix_args);
protected:
public:
    CoroDispatcherBase(Coroutine<FuncType> *coro_ptr, arg_to_view<Args> &&...args, uint dispatch_size) noexcept
        : _coro{std::move(coro_ptr)},
          _args{std::make_tuple(std::move(args)...)} {
    }
    [[nodiscard]] virtual bool all_dispatched() const noexcept = 0;
    [[nodiscard]] virtual bool all_done() const noexcept = 0;
    [[nodiscard]] CoroAwait<FuncType> await_step() noexcept;
    [[nodiscard]] CoroAwait<FuncType> await_all() noexcept;
};

template<typename FrameRef, typename... Args>
class WavefrontCoroDispatcher : public CoroDispatcherBase<void(FrameRef, Args...)> {
private:
    using FrameType = std::remove_reference_t<FrameRef>;
    Shader1D<Buffer<uint>, Buffer<uint>, Buffer<FrameType>, uint, uint, Args...> _gen_shader;
    luisa::vector<Shader1D<Buffer<uint>, Buffer<uint>, Buffer<FrameType>, uint, uint, Args...>> _resume_shaders;
    Shader1D<Buffer<uint>, Buffer<uint>, uint> _count_prefix_shader;
    Shader1D<Buffer<uint>, Buffer<uint>, Buffer<FrameType>, uint> _gather_shader;
    Shader1D<> _initialize_shader;
    Shader1D<> _compact_shader;
    compute::Buffer<FrameType> _frame;
    compute::Buffer<uint> _resume_index;
    compute::Buffer<uint> _resume_count;
    ///offset calculate from count, will be end after gathering
    compute::Buffer<uint> _resume_offset;
    compute::Buffer<uint> _global_buffer;
    luisa::vector<uint> _host_count;
    luisa::vector<uint> _host_offset;
    bool _host_empty;
    bool _dispatch_counter;
    uint _max_sub_coro;
    uint _max_frame_count;
    void _await_step(Stream &stream) noexcept;
public:
    //TODO: check async problem with these two functions
    bool all_dispatched() const noexcept;
    bool all_done() const noexcept;

    WavefrontCoroDispatcher(Coroutine<void(FrameRef, Args...)> *coroutine,
                            Device &device, arg_to_view<Args>... args, uint dispatch_size, uint max_frame_count = 2000000) noexcept
        : CoroDispatcherBase<void(FrameRef, Args...)>{coroutine, args..., dispatch_size},
          _max_frame_count{max_frame_count} {
        uint max_sub_coro = coroutine->_sub_builder.size() + 1;
        _max_sub_coro = max_sub_coro;
        _resume_index = device.create_buffer<uint>(max_frame_count);
        _resume_count = device.create_buffer<uint>(max_sub_coro);
        _resume_offset = device.create_buffer<uint>(max_sub_coro);
        Kernel1D gen_kernel = [&](BufferUInt index, BufferUInt count, Var<Buffer<FrameType>> frame_buffer, UInt st_task_id, UInt n, Var<Args>... args) {
            auto x = dispatch_x();
            $if (x >= n) {
                $return();
            };
            auto frame_id = index->read(x);
            auto frame = frame_buffer->read(frame_id);
            initialize_coroframe(frame, st_task_id + x);
            count->atomic(0).fetch_add(-1u);
            (*coroutine)(frame, args...);
            frame_buffer->write(frame_id, frame);
            auto nxt = $read_promise(frame, "resume_id");
            count->atomic(nxt).fetch_add(1);
        };
        _gen_shader = device.compile(gen_kernel);
        _resume_shaders.resize(max_sub_coro);
        for (int i = 1; i <= max_sub_coro; ++i) {
            auto resume_kernel = [&](BufferUInt index, BufferUInt count, Var<Buffer<FrameType>> frame_buffer, UInt n, Args... args) {
                auto x = dispatch_x();
                $if (x >= n) {
                    return;
                };
                auto frame_id = index->read(x);
                auto frame = frame_buffer->read(frame_id);
                auto prev = $read_promise(frame, "resume_id");
                count->atomic(i).fetch_add(-1u);
                (*coroutine)[i](frame, args...);
                auto nxt = $read_promise(frame, "resume_id");
                count->atomic(nxt).fetch_add(1);
            };
            _resume_shaders[i] = device.compile(resume_kernel);
        }
        auto _prefix_kernel = [&](BufferUInt count, BufferUInt prefix, UInt n) {
            /*_global_buffer->write(0, 0);
            sync_block();
            auto x = dispatch_x();
            auto val = ite(x < n, count->read(x), 0u);
            auto sum = warp_active_sum(val);
            auto w_pre = warp_prefix_sum(val);
            UInt res = 0u;
            $if (warp_is_first_active_lane()) {
                res = _global_buffer->atomic(0).fetch_add(sum);
            };
            res = warp_read_first_active_lane(res);
            prefix->write(x, res + w_pre);*/
            $if (dispatch_x() == 0) {
                auto pre = def(0u);
                $for (i, 0u, _max_sub_coro) {
                    auto val = count->read(i);
                    prefix->write(i, pre);
                    pre = pre + val;
                };
            };
        };
        _count_prefix_shader = device.compile(_prefix_kernel);
        auto _collect_kernel = [](BufferUInt index, BufferUInt prefix, Var<Buffer<FrameType>> frame_buffer, UInt n) {
            auto x = dispatch_x();
            auto frame = frame_buffer->read(x);
            auto r_id = $read_promise(frame, "resume_id");
            auto q_id = prefix->atomic(r_id).fetch_add(1);
            index->write(q_id, x);
        };
        _gather_shader = device.compile(_collect_kernel);
    }
};
template<typename T>
class ThreadCoroDispatcher : public CoroDispatcherBase<T> {
};
// a simple wrap that helps submit a coroutine dispatch to the stream
enum struct CmdTag {
    AWAIT_STEP,
    AWAIT_ALL
};
template<typename T>
class CoroAwait {

    friend class CoroDispatcherBase<T>;

public:


private:
    CmdTag _tag;
    CoroDispatcherBase<T> *_dispatcher;

private:
    CoroAwait(CmdTag tag,
              CoroDispatcherBase<T> *dispatcher) noexcept
        : _tag{tag}, _dispatcher{dispatcher} {}
public:
    void operator()(Stream &stream) &&noexcept;
};

}// namespace coro

template<typename T>
struct luisa::compute::detail::is_stream_event_impl<coro::CoroAwait<T>> : std::true_type {};
}// namespace luisa::compute
