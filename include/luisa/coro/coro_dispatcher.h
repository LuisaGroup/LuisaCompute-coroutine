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
    std::tuple<Args...> _args;
    Coroutine<FuncType> *_coro;
    virtual void _await_step(Stream &stream) noexcept = 0;
    virtual void _await_all(Stream &stream) noexcept;

protected:
public:
    CoroDispatcherBase(Coroutine<FuncType> *coro, Args &&...args) noexcept
        : _coro{std::move(coro)},
          _args{std::make_tuple(std::move(args))} {
    }
    [[nodiscard]] virtual bool all_dispatched() const noexcept = 0;
    [[nodiscard]] virtual bool all_finished() const noexcept = 0;
    [[nodiscard]] CoroAwait<FuncType> await_step() noexcept;
    [[nodiscard]] CoroAwait<FuncType> await_all() noexcept;
};

template<typename FrameRef, typename... Args>
class WavefrontCoroDispatcher : public CoroDispatcherBase<void(FrameRef, Args...)> {
private:
    using FrameType = std::remove_reference_t<FrameRef>;
    Shader1D<> _gen_shader;
    luisa::vector<Shader1D<>> _resume_shaders;
    luisa::unordered_map<uint, uint> _projection;//user suspend_id to internal resume_id
    Shader1D<> collect_shader;
    Buffer<FrameType> _frame;
    Buffer<uint> _resume_index;
    Buffer<uint> _resume_count;
    luisa::vector<uint> _host_count;
    bool _host_empty;
    uint _max_frame_count;
    uint _current_offset;
    void _await_step(Stream &stream) noexcept;
public:
    //TODO: check async problem with these two functions
    bool all_dispatched() const noexcept;
    bool all_finished() const noexcept;

    WavefrontCoroDispatcher(Coroutine<void(FrameRef, Args...)> *coro,
                            Device &device, uint max_frame_count = 2000000) noexcept
        : CoroDispatcherBase<void(FrameRef, Args...)>{coro},
          _max_frame_count{max_frame_count},
          _current_offset{0} {
        uint max_sub_coro = coro->_sub_builder.size() + 1;
        _resume_index = device.create_buffer<uint>(max_frame_count);
        _resume_count = device.create_buffer<uint>(max_sub_coro);
        Kernel1D gen_kernel = [](Buffer<uint> index, Buffer<uint> count, Buffer<FrameType> frame, Args... args) {
            uint x = dispatch_x();
            uint frame_id = index->read(x);
            FrameType frame = frame->read(index);
            (*coro)(frame, args);
            auto nxt = $read_promise(frame, "resume_id");
            //TODO:use transform provide compacted index representation;
            //TODO:use constant if possible
            //modify count
        };
        _gen_shader = device.compile(gen_kernel);
        _resume_shaders.resize(max_sub_coro + 1);
        for (int i = 1; i <= max_sub_coro; ++i) {
            auto resume_kernel = [&](Buffer<uint> index, Buffer<uint> count, Buffer<FrameType> frame) {
                uint x = dispatch_x();
                uint frame_id = index->read(x);
                FrameType frame = frame.read(index);
                (*coro)[i](frame);
            };
            _resume_shaders[i] = device.compile(resume_kernel);
        }
        auto _collect_kernel = []() {

        };
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
    void operator()(Stream &stream) && noexcept;
};

}// namespace coro

template<typename T>
struct luisa::compute::detail::is_stream_event_impl<coro::CoroAwait<T>> : std::true_type {};
}// namespace luisa::compute
