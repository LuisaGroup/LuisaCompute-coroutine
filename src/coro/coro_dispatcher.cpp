#include <luisa/coro/coro_dispatcher.h>

namespace luisa::compute::inline coro {
template<typename Frame, typename... T>
void CoroDispatcherBase<void(Frame, T...)>::_await_all(Stream &stream) noexcept {
    while (!this->all_done()) {
        this->_await_step(stream);
    }
}
template<typename Frame, typename... T>
CoroAwait<void(Frame, T...)> CoroDispatcherBase<void(Frame, T...)>::await_step() noexcept {
    return CoroAwait<void(Frame, T...)>{CmdTag::AWAIT_STEP, this};
}
template<typename Frame, typename... T>
CoroAwait<void(Frame, T...)> CoroDispatcherBase<void(Frame, T...)>::await_all() noexcept {
    return CoroAwait<void(Frame, T...)>{CmdTag::AWAIT_ALL, this};
}
template<typename FrameType, typename... Args>
template<size_t dim, typename... T>
detail::ShaderInvoke<dim> CoroDispatcherBase<void(FrameType, Args...)>::call_shader(Shader<dim, T..., Args...> shader, T &&...prefix_args) {
    auto invoke = shader.partial_invoke(std::forward<T>(prefix_args)...);
    for (auto &&arg : _args) {
        invoke << arg;
    }
    return invoke;
}
template<typename T>
void CoroAwait<T>::operator()(Stream &stream) &&noexcept {
    switch (_tag) {
        case CmdTag::AWAIT_STEP: this->_dispatcher->_await_step(stream); break;
        case CmdTag::AWAIT_ALL: this->_dispatcher->_await_all(stream); break;
    }
}
template<typename FrameRef, typename... Args>
void WavefrontCoroDispatcher<FrameRef, Args...>::_await_step(Stream &stream) noexcept {

    stream << _count_prefix_shader(_resume_count, _resume_offset, _max_sub_coro).dispatch(1u);
    stream << _gather_shader(_resume_index, _resume_offset, _frame, _max_frame_count).dispatch(_max_frame_count);
    if (_host_count[0] > _max_frame_count / 2 && !all_dispatched()) {
        stream << call_shader(_gen_shader, _resume_index.view(_host_offset[0], _host_count[0]), _resume_count, _frame, _max_frame_count).dispatch();

    } else {
        for (uint i = 1; i <= _max_sub_coro; i++) {
            if (_host_count[i] > 0) {
                stream << call_shader(_resume_shaders[i], _resume_index.view(_host_offset[i], _host_count[i]),
                                      _resume_count, _frame, _max_frame_count)
                              .dispatch(_host_count[i]);
            }
        }
    }
    auto host_update = [&] {
        _host_empty = true;
        auto sum = 0u;
        for (uint i = 0; i <= _max_sub_coro; i++) {
            _host_offset[i] = sum;
            sum += _host_count[i];
            _host_empty = _host_empty && (i == 0 || _host_count[i] == 0);
        }
    };
    stream << _resume_count.view(0, _max_sub_coro).copy_to(_host_count.data())
           << host_update();
};
template<typename FrameRef, typename... Args>
bool WavefrontCoroDispatcher<FrameRef, Args...>::all_dispatched() const noexcept {
    return this->_dispatch_size == this->_dispatch_counter;
};
template<typename FrameRef, typename... Args>
bool WavefrontCoroDispatcher<FrameRef, Args...>::all_done() const noexcept {
    return all_dispatched() && _host_empty;
};

}// namespace luisa::compute::inline coro
