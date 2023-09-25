#include <luisa/coro/coro_dispatcher.h>

namespace luisa::compute::inline coro {

void CoroDispatcher::_await_all(Stream &stream) noexcept {
    while (!this->all_dispatched()) {
        this->_await_step(stream);
    }
}

CoroAwait CoroDispatcher::await_step() noexcept {
    return CoroAwait{CoroAwait::Tag::AWAIT_STEP, this};
}

CoroAwait CoroDispatcher::await_all() noexcept {
    return CoroAwait{CoroAwait::Tag::AWAIT_ALL, this};
}

CoroAwait::CoroAwait(CoroAwait::Tag tag,
                     CoroDispatcher *dispatcher) noexcept
    : _tag{tag}, _dispatcher{dispatcher} {}

void CoroAwait::operator()(Stream &stream) && noexcept {
    switch (_tag) {
        case Tag::AWAIT_STEP: _dispatcher->_await_step(stream); break;
        case Tag::AWAIT_ALL: _dispatcher->_await_all(stream); break;
    }
}

}// namespace luisa::compute::inline coro
