#pragma once

#include <luisa/core/dll_export.h>
#include <luisa/runtime/stream_event.h>
#include <luisa/runtime/stream.h>
#include <luisa/runtime/rhi/command_encoder.h>
#include <luisa/core/stl.h>
#include <luisa/runtime/shader.h>
#include <luisa/dsl/builtin.h>
namespace luisa::compute {

class Stream;
inline namespace coro {
template<typename T>
struct prototype_to_coro_dispatcher {
    using type = T;
};

template<typename T>
struct prototype_to_coro_dispatcher<Buffer<T>> {
    using type = BufferView<T>;
};

template<typename T>
struct prototype_to_coro_dispatcher<Image<T>> {
    using type = ImageView<T>;
};

template<typename T>
struct prototype_to_coro_dispatcher<Volume<T>> {
    using type = VolumeView<T>;
};

template<typename T>
struct prototype_to_coro_dispatcher<SOA<T>> {
    using type = SOAView<T>;
};

template<typename T>
using prototype_to_coro_dispatcher_t = typename prototype_to_coro_dispatcher<T>::type;

template<typename T>
class CoroAwait;
template<typename T>
class CoroDispatcherBase {
    static_assert(always_false_v<T>);
};
const uint token_mask=0x7fffffff;
template<typename FrameRef, typename... Args>
class CoroDispatcherBase<void(FrameRef, Args...)> : public concepts::Noncopyable {
    using FuncType = void(FrameRef, Args...);
    friend class CoroAwait<FuncType>;
public:

    luisa::queue<luisa::unique_ptr<ShaderDispatchCommand>> _dispatcher;
protected:
    Printer _printer;
    std::tuple<prototype_to_coro_dispatcher_t<Args>...> _args;
    Coroutine<FuncType> *_coro;
    virtual void _await_step(Stream &stream) noexcept = 0;
    virtual void _await_all(Stream &stream) noexcept;
    uint _dispatch_size;
    Device _device;
    ///helper function for calling a shader by omitting suffix coroutine args
    template<size_t dim, typename... T>
    detail::ShaderInvoke<dim> call_shader(Shader<dim, T..., Args...> &shader, prototype_to_coro_dispatcher_t<T>...prefix_args);

protected:
public:
    CoroDispatcherBase(Coroutine<FuncType> *coro_ptr,Device& device) noexcept
        : _coro{std::move(coro_ptr)},_device{device} , _printer{device}{
        _printer.reset();
    }
    [[nodiscard]] virtual bool all_dispatched() const noexcept = 0;
    [[nodiscard]] virtual bool all_done() const noexcept = 0;
    [[nodiscard]] CoroAwait<FuncType> await_step() noexcept;
    [[nodiscard]] CoroAwait<FuncType> await_all() noexcept;
    void operator()(prototype_to_coro_dispatcher_t<Args>...args, uint dispatch_size) noexcept {
        _args=std::make_tuple(std::forward<prototype_to_coro_dispatcher_t<Args>>(args)...);
        _dispatch_size=dispatch_size;
    }
};


template<typename FrameRef, typename... Args>
class WavefrontCoroDispatcher: public CoroDispatcherBase<void(FrameRef, Args...)> {
private:
    using FrameType = std::remove_reference_t<FrameRef>;
    Shader1D<Buffer<uint>, Buffer<uint>, Buffer<FrameType>, uint, uint, Args...> _gen_shader;
    luisa::vector<Shader1D<Buffer<uint>, Buffer<uint>, Buffer<FrameType>, uint, Args...>> _resume_shaders;
    Shader1D<Buffer<uint>, Buffer<uint>, uint> _count_prefix_shader;
    Shader1D<Buffer<uint>, Buffer<uint>, Buffer<FrameType>, uint> _gather_shader;
    Shader1D<Buffer<uint>, Buffer<FrameType>,uint> _initialize_shader;
    Shader1D<Buffer<uint>,Buffer<FrameType>,uint,uint> _compact_shader;
    compute::Buffer<FrameType> _frame;
    compute::Buffer<uint> _resume_index;
    compute::Buffer<uint> _resume_count;
    ///offset calculate from count, will be end after gathering
    compute::Buffer<uint> _resume_offset;
    compute::Buffer<uint> _global_buffer;
    luisa::vector<uint> _host_count;
    luisa::vector<uint> _host_offset;
    bool _host_empty;
    uint _dispatch_counter;
    uint _max_sub_coro;
    uint _max_frame_count;
    bool _debug;
    void _await_step(Stream &stream) noexcept;
public:
    //TODO: check async problem with these two functions
    bool all_dispatched() const noexcept;
    bool all_done() const noexcept;

    WavefrontCoroDispatcher(Coroutine<void(FrameRef, Args...)> *coroutine, Device &device, Stream& stream,
                            uint max_frame_count = 2000000, bool debug=false) noexcept
        : CoroDispatcherBase<void(FrameRef, Args...)>{coroutine,device},
          _max_frame_count{max_frame_count}, _debug{debug} {
        uint max_sub_coro = coroutine->suspend_count() + 1;
        _max_sub_coro = max_sub_coro;
        _resume_index = device.create_buffer<uint>(max_frame_count);
        _resume_count = device.create_buffer<uint>(max_sub_coro);
        _resume_offset = device.create_buffer<uint>(max_sub_coro);
        _global_buffer = device.create_buffer<uint>(1);
        _host_empty=true;
        _dispatch_counter=0;
        _frame = device.create_buffer<FrameType>(max_frame_count);
        _host_offset.resize(max_sub_coro);
        _host_count.resize(max_sub_coro);
        for(auto i=0u;i<max_sub_coro;i++){
            if(i) {
                _host_count[i] = 0;
                _host_offset[i] = max_frame_count;
            }
            else{
                _host_count[i] = max_frame_count;
                _host_offset[i] = 0;
            }
        }
        Kernel1D gen_kernel = [&](BufferUInt index, BufferUInt count, Var<Buffer<FrameType>> frame_buffer, UInt st_task_id, UInt n, Var<Args>... args) {
            auto x = dispatch_x();
            $if (x >= n) {
                $return();
            };
            //auto frame_id = index->read(x);
            auto offset=_max_frame_count-count.read(0);
            auto frame_id = offset+x;
            auto frame = frame_buffer.read(frame_id);
            initialize_coroframe(frame, def<uint3>(st_task_id + x,0,0));
            count.atomic(0).fetch_add(-1u);
            (*coroutine)(frame, args...);
            frame_buffer.write(frame_id, frame);
            auto nxt = read_promise<uint>(frame, "token")&token_mask;
            count.atomic(nxt).fetch_add(1u);
        };
        _gen_shader = device.compile(gen_kernel);
        _resume_shaders.resize(max_sub_coro);
        for (int i = 1; i < max_sub_coro; ++i) {
            Kernel1D resume_kernel = [&](BufferUInt index, BufferUInt count, Var<Buffer<FrameType>> frame_buffer, UInt n, Var<Args>... args) {
                auto x = dispatch_x();
                $if (x >= n) {
                    $return();
                };
                auto frame_id = index.read(x);
                auto frame = frame_buffer.read(frame_id);
                count.atomic(i).fetch_add(-1u);
                (*coroutine)[i](frame, args...);
                auto nxt = read_promise<uint>(frame, "token")&token_mask;
                frame_buffer.write(frame_id,frame);
                if(debug)
                    this->_printer.info("resume kernel {} : id {} goto kernel {}", i, frame_id,nxt);
                count.atomic(nxt).fetch_add(1u);
            };
            _resume_shaders[i] = device.compile(resume_kernel);
        }
        Kernel1D _prefix_kernel = [&](BufferUInt count, BufferUInt prefix, UInt n) {
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
                    auto val = count.read(i);
                    prefix.write(i, pre);
                    pre = pre + val;
                };
            };
        };
        _count_prefix_shader = device.compile(_prefix_kernel);
        Kernel1D _collect_kernel = [](BufferUInt index, BufferUInt prefix, Var<Buffer<FrameType>> frame_buffer, UInt n) {
            auto x = dispatch_x();
            auto frame = frame_buffer.read(x);
            auto r_id = read_promise<uint>(frame, "token")&token_mask;
            auto q_id = prefix.atomic(r_id).fetch_add(1u);
            index.write(q_id, x);
        };
        _gather_shader = device.compile(_collect_kernel);
        Kernel1D _compact_kernel = [&](BufferUInt index, Var<Buffer<FrameType>> frame_buffer, UInt empty_offset, UInt n){
            _global_buffer->write(0u, 0u);
            auto x = dispatch_x();
            $if(empty_offset+x<n){
                auto frame = frame_buffer.read(empty_offset+x);
                $if((read_promise<uint>(frame,"token")&token_mask)!=0){
                    auto res = _global_buffer->atomic(0).fetch_add(1u);
                    auto slot= index.read(res);
                    auto empty=frame_buffer.read(slot);
                    frame_buffer.write(slot,frame);
                    frame_buffer.write(empty_offset,empty);
                };
            };
        };
        _compact_shader = device.compile(_compact_kernel);
        Kernel1D _initialize_kernel = [&](BufferUInt count,Var<Buffer<FrameType>> frame_buffer,UInt n){
            auto x = dispatch_x();
            $if(x<n){
                auto frame = frame_buffer.read(x);
                initialize_coroframe(frame, def<uint3>(0,0,0));
            };
            $if(x<max_sub_coro){
                count.write(x,ite(x==0,max_frame_count,0u));
            };
        };
        _initialize_shader = device.compile(_initialize_kernel);
        stream<<_initialize_shader(_resume_count,_frame,_max_frame_count).dispatch(_max_frame_count);
    }
};
template<typename FrameRef, typename... Args>
class PersistentCoroDispatcher: public CoroDispatcherBase<void(FrameRef, Args...)> {
private:
    using FrameType = std::remove_reference_t<FrameRef>;
    Shader1D<Buffer<uint>, uint, Args...> _pt_shader;
    Shader1D<Buffer<uint>> _clear_shader;
    Buffer<uint> _global;
    uint _max_sub_coro;
    uint _max_thread_count;
    uint _block_size;
    bool _dispatched;
    bool _done;
    void _await_step(Stream &stream) noexcept;
    void _await_all(Stream &stream) noexcept;
    bool _debug;
public:
    bool all_dispatched() const noexcept;
    bool all_done() const noexcept;
    PersistentCoroDispatcher(Coroutine<void(FrameRef, Args...)> *coroutine, Device &device, Stream& stream,
                            uint max_thread_count=1024*128,uint block_size=128, uint fetch_size=128, bool debug=false) noexcept
        : CoroDispatcherBase<void(FrameRef, Args...)>{coroutine,device},
          _max_thread_count{max_thread_count}, _block_size{block_size}, _debug{debug} {
        _global=device.create_buffer<uint>(1);
        uint max_sub_coro = coroutine->suspend_count() + 1;
        _max_sub_coro = max_sub_coro;
        _dispatched = false;
        _done = false;
        Kernel1D main_kernel=[&](BufferUInt global, UInt dispatch_size, Var<Args>...args){
            set_block_size(block_size, 1, 1);
            auto q_fac=1u;
            auto shared_queue_size = block_size*q_fac;
            Shared<FrameType> frames{shared_queue_size};
            Shared<uint> path_id{shared_queue_size};
            Shared<uint> work_counter{max_sub_coro};
            Shared<uint> work_offset{2u};
            Shared<uint> workload{2};
            Shared<uint> work_stat{2};//0 max_count,1 max_id
            //Shared<uint> tag_counter{use_tag_sort ? pipeline().surfaces().size() : 0};
            //Shared<uint> tag_offset{pipeline().surfaces().size()};
            initialize_coroframe(frames[thread_x()], def<uint3>(0,0,0));
            $if(thread_x() < max_sub_coro) {
                $if(thread_x() == 0){
                    work_counter[thread_x()] = shared_queue_size;
                }
                $else {
                    work_counter[thread_x()] = 0u;
                };
            };
            workload[0] = 0;
            workload[1] = 0;
            Shared<uint> rem_global{1};
            Shared<uint> rem_local{1};
            rem_global[0] = 1u;
            rem_local[0] = 0u;
            sync_block();
            auto count = def(0);
            uint count_limit=-1;
            if(_debug){
                count_limit=20;
            }

            $while((rem_global[0] != 0u | rem_local[0] != 0u) & (count!= count_limit)) {
                sync_block();//very important, synchronize for condition
                rem_local[0] = 0u;
                count += 1;
                work_stat[0] = 0;
                work_stat[1] = -1;
                /* $if(thread_x() < (uint)KERNEL_COUNT) {//clear counter
                work_counter[thread_x()] = 0u;
            };
            sync_block();
            $for(index, 0u, q_factor) {
                for (auto i = 0u; i < KERNEL_COUNT; ++i) {//count the kernels
                    auto state = path_state[index*block_size+thread_x()];
                    $if(state.kernel_index == i) {
                        if (i != (uint)INVALID) {
                            rem_local[0] = 1u;
                        } else {
                            $if(workload[0] < workload[1]) {
                                rem_local[0] = 1u;
                            };
                        }
                        work_counter.atomic(i).fetch_add(1u);
                    };
                }
            };*/
                sync_block();
                $if(thread_x() == block_size - 1) {
                    $if((workload[0] >= workload[1]) & (rem_global[0]==1u)) {//fetch new workload
                        workload[0] = global.atomic(0u).fetch_add(block_size * fetch_size);
                        if(_debug)
                            this->_printer.info("block {}, fetch workload: {}", block_x(), workload[0]);
                        workload[1] = min(workload[0] + block_size * fetch_size, dispatch_size);
                        $if(workload[0] >= dispatch_size) {
                            rem_global[0] = 0u;
                        };
                    };
                };
                sync_block();
                $if(thread_x() < max_sub_coro) {//get max
                    $if((workload[0] < workload[1]) | (thread_x() != 0u)) {
                        $if(work_counter[thread_x()] != 0) {
                            rem_local[0] = 1u;
                            work_stat.atomic(0).fetch_max(work_counter[thread_x()]);
                        };
                    };
                    if(_debug)
                        this->_printer.info("work counter {} of block {}: {}", thread_x(), block_x(), work_counter[thread_x()]);

                };
                sync_block();
                $if(thread_x() < max_sub_coro){//get argmax
                    $if((work_stat[0] == work_counter[thread_x()]) & ((workload[0] < workload[1]) | (thread_x() != 0u))) {
                        work_stat[1] = thread_x();
                    };
                };
                sync_block();
                work_offset[0] = 0;
                work_offset[1] = 0;
                sync_block();
                $for(index, 0u, q_fac) {//collect indices
                    auto frame = frames[index * block_size + thread_x()];
                    $if((read_promise<uint>(frame,"token")&token_mask) == work_stat[1]) {
                        auto id = work_offset.atomic(0).fetch_add(1u);
                        path_id[id] = index * block_size + thread_x();
                    };
                };
                auto gen_st = workload[0];
                sync_block();
                auto pid = def(0u);
                pid = path_id[thread_x()];
                auto launch_condition = def(true);
                launch_condition = (thread_x() < work_offset[0]);
                $if(launch_condition) {
                    $switch(read_promise<uint>(frames[pid],"token")&token_mask) {
                        $case(0u){
                            $if(gen_st + thread_x() < workload[1]) {
                                work_counter.atomic(0u).fetch_sub(1u);
                                auto work_id = gen_st + thread_x();
                                initialize_coroframe(frames[pid], def<uint3>(gen_st + thread_x(),0,0));
                                (*coroutine)(frames[pid],args...);//only work when kernel 0s are continue
                                auto nxt=read_promise<uint>(frames[pid],"token")&token_mask;
                                work_counter.atomic(nxt).fetch_add(1u);
                                if(_debug)
                                    this->_printer.info("gen_load_st {}, work_id {}, goto {}", gen_st,work_id, nxt);
                                workload.atomic(0).fetch_add(1u);
                            };
                        };
                        for(auto i=1u;i < max_sub_coro;++i){
                            $case (i){
                                work_counter.atomic(i).fetch_sub(1u);
                                (*coroutine)[i](frames[pid],args...);
                                work_counter.atomic(read_promise<uint>(frames[pid],"token")&token_mask).fetch_add(1u);
                                if(_debug)
                                    this->_printer.info("resume kernel {} on block {}: id {} goto kernel {}", i, block_x(),read_promise<uint3>(frames[pid],"coro_id"),read_promise<uint>(frames[pid],"token")&token_mask);
                            };
                        }
                    };
                };

            };
            $if(count == count_limit) {
                this->_printer.info("block_id{},thread_id {}, loop not break! local:{}, global:{}",block_x(),thread_x(), rem_local[0], rem_global[0]);
                $if(thread_x() < max_sub_coro){
                    this->_printer.info("work rem: id {}, size {}", thread_x(), work_counter[thread_x()]);
                };
            };
        };
        _pt_shader=device.compile(main_kernel);
        Kernel1D clear=[&](BufferUInt global){
            global->write(dispatch_x(),0u);
        };
        _clear_shader = device.compile(clear);
        stream<<_clear_shader(_global).dispatch(1u);
    }
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


namespace luisa::compute::inline coro {
template<typename FrameRef, typename... Args>
void CoroDispatcherBase<void(FrameRef, Args...)>::_await_all(Stream &stream) noexcept {
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
detail::ShaderInvoke<dim> CoroDispatcherBase<void(FrameType, Args...)>::call_shader(Shader<dim, T..., Args...> &shader,
                                                                                    prototype_to_coro_dispatcher_t<T>...prefix_args) {
    auto invoke = shader.partial_invoke(prefix_args...);
    std::apply([&invoke](prototype_to_coro_dispatcher_t<Args>...args){
        static_cast<void>((invoke << ... << args));
    }, _args);
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
    if(_debug)
        for(int i=0;i<_max_sub_coro;++i){
            LUISA_INFO("kernel {}: total {}",i,_host_count[i]);
        }
    stream << _count_prefix_shader(_resume_count, _resume_offset, _max_sub_coro).dispatch(1u);
    stream << _gather_shader(_resume_index, _resume_offset, _frame, _max_frame_count).dispatch(_max_frame_count);
    if (_host_count[0] > _max_frame_count / 2 && !all_dispatched()) {
        if(_host_count[0]!=_max_frame_count) {
            stream << _compact_shader(_resume_index, _frame, _host_count[0], _max_frame_count).dispatch(_max_frame_count - _host_count[0]);
        }
        stream << this->template call_shader<1,Buffer<uint>,Buffer<uint>,Buffer<FrameType>,uint,uint>
            (_gen_shader, _resume_index.view(_host_offset[0], _host_count[0]), _resume_count, _frame, _dispatch_counter,_max_frame_count).dispatch(_host_count[0]);
        _dispatch_counter+=_host_count[0];
    } else {
        for (uint i = 1; i < _max_sub_coro; i++) {
            if (_host_count[i] > 0) {
                stream << this->template call_shader<1,Buffer<uint>,Buffer<uint>,Buffer<FrameType>,uint>
                    (_resume_shaders[i], _resume_index.view(_host_offset[i], _host_count[i]),
                                      _resume_count, _frame, _max_frame_count)
                              .dispatch(_host_count[i]);
            }
        }
    }
    auto host_update = [&] {
        _host_empty = true;
        auto sum = 0u;
        for (uint i = 0; i < _max_sub_coro; i++) {
            _host_offset[i] = sum;
            sum += _host_count[i];
            _host_empty = _host_empty && (i == 0 || _host_count[i] == 0);
        }
    };
    stream << _resume_count.view(0, _max_sub_coro).copy_to(_host_count.data())
           << host_update;
    if(_debug){
        stream<<this->_printer.retrieve();
    }
    stream << synchronize();
};
template<typename FrameRef, typename... Args>
bool WavefrontCoroDispatcher<FrameRef, Args...>::all_dispatched() const noexcept {
    return this->_dispatch_size == this->_dispatch_counter;
};
template<typename FrameRef, typename... Args>
bool WavefrontCoroDispatcher<FrameRef, Args...>::all_done() const noexcept {
    return all_dispatched() && _host_empty;
};

template<typename FrameRef, typename... Args>
void PersistentCoroDispatcher<FrameRef, Args...>::_await_step(Stream &stream) noexcept {
    LUISA_ERROR("PersistentCoroDispatcher can only be used with await_all!");
}
template<typename FrameRef, typename... Args>
void PersistentCoroDispatcher<FrameRef, Args...>::_await_all(Stream &stream) noexcept {
    stream<<this->template call_shader<1,Buffer<uint>,uint>(_pt_shader,_global,this->_dispatch_size).dispatch(_max_thread_count)
        <<[&]{_done=true;};
    stream<<this->_printer.retrieve();
    stream<<synchronize();
}

template<typename FrameRef, typename... Args>
bool PersistentCoroDispatcher<FrameRef, Args...>::all_dispatched() const noexcept {
    return _dispatched;
}
template<typename FrameRef, typename... Args>
bool PersistentCoroDispatcher<FrameRef, Args...>::all_done() const noexcept {
    return _done;
}
}// namespace luisa::compute::inline coro
