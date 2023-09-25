#pragma once

#include <luisa/core/dll_export.h>
#include <luisa/runtime/stream_event.h>
#include <luisa/runtime/stream.h>
#include <luisa/coro/coro_graph.h>

namespace luisa::compute {

class Stream;

inline namespace coro {

class CoroAwait;

class LC_CORO_API CoroDispatcher : public concepts::Noncopyable {

    friend class CoroAwait;

public:
    struct ArgumentBuffer {
        luisa::vector<std::byte> buffer;// [Argument...] + [uniforms...]
        size_t count;
        // TODO: provide a default encoder for argument buffer
    };

private:
    luisa::shared_ptr<const CoroGraph> _graph;
    ArgumentBuffer _argument_buffer;
    uint3 _dispatch_size;

protected:
    virtual void _await_step(Stream &stream) noexcept = 0;
    virtual void _await_all(Stream &stream) noexcept;

protected:
    CoroDispatcher(luisa::shared_ptr<const CoroGraph> graph,
                   ArgumentBuffer argument_buffer,
                   uint3 dispatch_size) noexcept
        : _graph{std::move(graph)},
          _argument_buffer{std::move(argument_buffer)},
          _dispatch_size{dispatch_size} {}

public:
    [[nodiscard]] virtual bool all_done() const noexcept = 0;
    [[nodiscard]] virtual bool all_dispatched() const noexcept = 0;
    [[nodiscard]] CoroAwait await_step() noexcept;
    [[nodiscard]] CoroAwait await_all() noexcept;
};

// a simple wrap that helps submit a coroutine dispatch to the stream
class LC_CORO_API CoroAwait {

    friend class CoroDispatcher;

public:
    enum struct Tag {
        AWAIT_STEP,
        AWAIT_ALL
    };

private:
    Tag _tag;
    CoroDispatcher *_dispatcher;

private:
    CoroAwait(Tag tag, CoroDispatcher *dispatcher) noexcept;

public:
    void operator()(Stream &stream) && noexcept;
};

}// namespace coro

LUISA_MARK_STREAM_EVENT_TYPE(coro::CoroAwait)

}// namespace luisa::compute
