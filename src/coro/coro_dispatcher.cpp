#include <luisa/coro/coro_dispatcher.h>

namespace luisa::compute::inline coro {
template<typename Frame,typename... T>
void CoroDispatcherBase<void(Frame,T...)>::_await_all(Stream &stream) noexcept {
    while (!this->all_dispatched()) {
        this->_await_step(stream);
    }
}
template<typename Frame,typename... T>
CoroAwait<void(Frame,T...)> CoroDispatcherBase<void(Frame,T...)>::await_step() noexcept {
    return CoroAwait<T>{CoroAwait::Tag::AWAIT_STEP, this};
}
template<typename Frame,typename ...T>
CoroAwait<void(Frame, T...)> CoroDispatcherBase<void(Frame,T...)>::await_all() noexcept {
    return CoroAwait<T>{CoroAwait::Tag::AWAIT_ALL, this};
}

template<typename T>
void CoroAwait<T>::operator()(Stream &stream) && noexcept {
    switch (_tag) {
        case CmdTag::AWAIT_STEP: this->_dispatcher->_await_step(stream); break;
        case CmdTag::AWAIT_ALL: this->_dispatcher->_await_all(stream); break;
    }
}
template<typename FrameRef, typename... Args>
void WavefrontCoroDispatcher<FrameRef, Args...>::_await_step(Stream &stream) noexcept {
    //catch_counter(_resume_count, _host_count);
    if (_host_count[0] > _max_frame_count / 2 && !all_dispatched()) {
        stream << _gather_shader(_resume_index, _frame, 0u).dispatch(_max_frame_count);
        auto invoke = _gen_shader.partial_invoke(_resume_index, _resume_count, _frame);
        for (auto &arg : this->_dispatcher.front()->arguments()) {
        }
        stream << invoke.dispatch(this->_dispatcher.front().dispatch_size());
    }
};
template<typename FrameRef, typename... Args>
bool WavefrontCoroDispatcher<FrameRef, Args...>::all_dispatched() const noexcept {
    return this->_dispatcher.empty();
};
template<typename FrameRef, typename... Args>
bool WavefrontCoroDispatcher<FrameRef, Args...>::all_finished() const noexcept {
    return all_dispatched() && _host_empty;
};


}// namespace luisa::compute::inline coro
