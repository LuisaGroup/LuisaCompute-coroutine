#pragma once

#include <luisa/core/dll_export.h>
#include <luisa/runtime/stream_event.h>
#include <luisa/runtime/stream.h>
#include <luisa/runtime/rhi/command_encoder.h>
#include <luisa/core/stl.h>
#include <luisa/runtime/shader.h>

#include <luisa/dsl/func.h>
#include <luisa/dsl/sugar.h>
#include <luisa/dsl/builtin.h>
#include <luisa/coro/radix_sort.h>
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
const uint token_mask = 0x7fffffff;
template<typename FrameRef, typename... Args>
class CoroDispatcherBase<void(FrameRef, Args...)> : public concepts::Noncopyable {
    using FuncType = void(FrameRef, Args...);
    friend class CoroAwait<FuncType>;
public:

    luisa::queue<luisa::unique_ptr<ShaderDispatchCommand>> _dispatcher;
protected:
    std::tuple<prototype_to_coro_dispatcher_t<Args>...> _args;
    Coroutine<FuncType> *_coro;
    virtual void _await_step(Stream &stream) noexcept = 0;
    virtual void _await_all(Stream &stream) noexcept;
    uint _dispatch_size;
    Device _device;
    ///helper function for calling a shader by omitting suffix coroutine args
    template<size_t dim, typename... T>
    detail::ShaderInvoke<dim> call_shader(Shader<dim, T..., Args...> &shader, prototype_to_coro_dispatcher_t<T>... prefix_args);

protected:
public:
    CoroDispatcherBase(Coroutine<FuncType> *coro_ptr, Device &device) noexcept
        : _coro{std::move(coro_ptr)}, _device{device}, _dispatch_size{} {}
    virtual ~CoroDispatcherBase() noexcept = default;
    [[nodiscard]] virtual bool all_dispatched() const noexcept = 0;
    [[nodiscard]] virtual bool all_done() const noexcept = 0;
    [[nodiscard]] CoroAwait<FuncType> await_step() noexcept;
    [[nodiscard]] CoroAwait<FuncType> await_all() noexcept;
    virtual void operator()(prototype_to_coro_dispatcher_t<Args>... args, uint dispatch_size) noexcept {//initialize the dispatcher
        _args = std::make_tuple(std::forward<prototype_to_coro_dispatcher_t<Args>>(args)...);
        _dispatch_size = dispatch_size;
    }
};

template<typename FrameRef, typename... Args>
class SimpleCoroDispatcher : public CoroDispatcherBase<void(FrameRef, Args...)> {
private:
    using FrameType = std::remove_reference_t<FrameRef>;
    Shader1D<uint, Args...> _shader;
    bool _done;
    void _await_step(Stream &stream) noexcept override {
        stream << this->template call_shader<1, uint>(_shader, this->_dispatch_size).dispatch(this->_dispatch_size);
        _done = true;
        stream << synchronize();
    }
public:
    SimpleCoroDispatcher(Coroutine<void(FrameRef, Args...)> *coroutine, Device &device,
                         uint max_frame_count) : CoroDispatcherBase<void(FrameRef, Args...)>{coroutine, device},
                                                 _done{false} {
        uint max_sub_coro = coroutine->suspend_count() + 1;
        Kernel1D run_kernel = [&](UInt n, Var<Args>... args) {
            set_block_size(128, 1, 1);
            auto x = dispatch_x();
            $if (x < n) {
                Var<FrameType> frame;
                initialize_coroframe(frame, def<uint3>(x, 0, 0));
                (*coroutine)(frame, args...);
                $loop {
                    auto token = read_promise<uint>(frame, "coro_token");
                    $if (token == 0x8000'0000) {
                        $break;
                    };
                    $switch (token) {
                        for (auto i = 1u; i < max_sub_coro; ++i) {
                            $case (i) {
                                (*coroutine)[i](frame, args...);
                            };
                        }
                    };
                };
            };
        };
        _shader = device.compile(run_kernel);
    }
    void operator()(prototype_to_coro_dispatcher_t<Args>... args, uint dispatch_size) noexcept {
        CoroDispatcherBase<void(FrameRef, Args...)>::operator()(args..., dispatch_size);
        _done = false;
    }
    bool all_dispatched() const noexcept override {
        return _done;
    }
    bool all_done() const noexcept override {
        return _done;
    }
};

struct WavefrontCoroDispatcherConfig {
    uint max_instance_count = 2_M;
    bool soa = true;
    bool sort = true;//use sort for coro token gathering
    bool compact = true;
    bool debug = false;
    uint hint_range=0xffff'ffff;
    luisa::vector<luisa::string> hint_fields;
};

template<typename FrameRef, typename... Args>
class WavefrontCoroDispatcher : public CoroDispatcherBase<void(FrameRef, Args...)> {

public:
    using Config = WavefrontCoroDispatcherConfig;

private:
    using FrameType = std::remove_reference_t<FrameRef>;
    using Container = compute::SOA<FrameType>;
    bool is_soa = true;
    bool sort_base_gather = false;
    bool use_compact = true;
    Shader1D<Buffer<uint>, Buffer<uint>, uint, Container, uint, uint, Args...> _gen_shader;
    luisa::vector<Shader1D<Buffer<uint>, Buffer<uint>, Container, uint, Args...>> _resume_shaders;
    Shader1D<Buffer<uint>, Buffer<uint>, uint> _count_prefix_shader;
    Shader1D<Buffer<uint>, Buffer<uint>, Container, uint> _gather_shader;
    Shader1D<Buffer<uint>, Container, uint> _initialize_shader;
    Shader1D<Buffer<uint>, Container, uint, uint> _compact_shader;
    Shader1D<Buffer<uint>, uint> _clear_shader;
    Container _frame;
    compute::Buffer<uint> _resume_index;
    compute::Buffer<uint> _resume_count;
    ///offset calculate from count, will be end after gathering
    compute::Buffer<uint> _resume_offset;
    compute::Buffer<uint> _global_buffer;
    compute::Buffer<uint> _debug_buffer;
    luisa::vector<uint> _host_count;
    luisa::vector<uint> _host_offset;
    bool _host_empty;
    uint _dispatch_counter;
    uint _max_sub_coro;
    uint _max_frame_count;
    bool _debug;
    void _await_step(Stream &stream) noexcept override;
    radix_sort::temp_storage _sort_temp_storage;
    radix_sort::instance<> _sort_token;
    radix_sort::instance<Buffer<uint>> _sort_hint;
    luisa::vector<bool> _have_hint;
    compute::Buffer<uint> _temp_key[2];
    compute::Buffer<uint> _temp_index;
    Stream &_stream;
public:
    bool all_dispatched() const noexcept;
    bool all_done() const noexcept;

    WavefrontCoroDispatcher(Coroutine<void(FrameRef, Args...)> *coroutine,
                            Device &device, Stream &stream,
                            const WavefrontCoroDispatcherConfig &config) noexcept
        : CoroDispatcherBase<void(FrameRef, Args...)>{coroutine, device},
          is_soa{config.soa},
          sort_base_gather{config.sort},
          use_compact{config.compact},
          _max_frame_count{config.max_instance_count},
          _stream{stream}, _debug{config.debug},
          _frame{device.create_soa<FrameType>(config.max_instance_count)} {
        /*if (device.backend_name() != "cuda") {//only cuda can sort
            sort_base_gather = false;
            hint_token = {};
            LUISA_INFO("Using wavefront dispatcher without cuda, the sorting will be disabled!");
        }*/
        bool use_sort = sort_base_gather||!config.hint_fields.empty();
        uint max_sub_coro = coroutine->suspend_count() + 1;
        _max_sub_coro = max_sub_coro;
        _resume_index = device.create_buffer<uint>(_max_frame_count);
        if (use_sort) {
            _temp_index = device.create_buffer<uint>(_max_frame_count);
            _temp_key[0] = device.create_buffer<uint>(_max_frame_count);
            _temp_key[1] = device.create_buffer<uint>(_max_frame_count);
        }
        _resume_count = device.create_buffer<uint>(max_sub_coro);
        _resume_offset = device.create_buffer<uint>(max_sub_coro);
        _global_buffer = device.create_buffer<uint>(1);
        if (_debug) {
            _debug_buffer = device.create_buffer<uint>(max_sub_coro);
        }
        _host_empty = true;
        _dispatch_counter = 0;
        _host_offset.resize(max_sub_coro);
        _host_count.resize(max_sub_coro);
        _have_hint.resize(max_sub_coro, false);
        for (auto &token : config.hint_fields) {
            auto id = coroutine->coro_tokens().find(token);
            if (id != coroutine->coro_tokens().end()) {
                LUISA_ASSERT(id->second<max_sub_coro, "coroutine token {} of id {} out of range {}", token,id->second,max_sub_coro);
                _have_hint[id->second] = true;
            }
            else
                LUISA_WARNING("coroutine token {} not found, hint disabled", token);
        }
        for (auto i = 0u; i < max_sub_coro; i++) {
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
            auto frame = _frame->read(index);
            return read_promise<uint>(frame, "coro_token") & token_mask;
        };
        Callable identical = [&](UInt index) {
            return index;
        };

        Callable keep_index = [&](UInt index,BufferUInt val) {
            return val.read(index);
        };
        Callable get_coro_hint = [&](UInt index,BufferUInt val) {
            if (!config.hint_fields.empty()) {
                auto id = keep_index(index,val);
                auto frame = _frame->read(id);
                auto x=read_promise<uint>(frame, "coro_hint");
                return x;
            } else {
                return def<uint>(0u);
            }
        };
        if (use_sort) {
            _sort_temp_storage = radix_sort::temp_storage(device, _max_frame_count, std::max(std::min(config.hint_range,128u), max_sub_coro));
        }
        if (sort_base_gather) {
            _sort_token = radix_sort::instance<>(device, _max_frame_count, _sort_temp_storage,
                                     &get_coro_token, &identical, &get_coro_token, 1, max_sub_coro);
        }
        if (!config.hint_fields.empty()) {
            if(config.hint_range<=128){
                _sort_hint = radix_sort::instance<Buffer<uint>>(device, _max_frame_count, _sort_temp_storage,
                                                    &get_coro_hint, &keep_index, &get_coro_hint, 1, config.hint_range);
            }
            else{
                auto highbit=0;
                while((config.hint_range>>highbit)!=1){
                    highbit++;
                }
                _sort_hint = radix_sort::instance<Buffer<uint>>(device, _max_frame_count, _sort_temp_storage,
                                                                &get_coro_hint, &keep_index, &get_coro_hint, 0, 128, 0, highbit);
            }
        }

        Kernel1D gen_kernel = [&](BufferUInt index, BufferUInt count, UInt offset, Var<Container> frame_buffer, UInt st_task_id, UInt n, Var<Args>... args) {
            auto x = dispatch_x();
            $if (x >= n) {
                $return();
            };
            UInt frame_id;
            if (!use_compact) {
                frame_id = index->read(x);
            } else {
                frame_id = offset + x;
            }
            Var<FrameType> frame;
            if (_debug) {
                frame = frame_buffer.read(frame_id);
                $if ((read_promise<uint>(frame, "coro_token") & token_mask) != 0u) {
                    device_log("wrong gen for frame {} at kernel {}when dispatch {}", frame_id, read_promise<uint>(frame, "coro_token"), dispatch_x());
                };
            }
            initialize_coroframe(frame, def<uint3>(st_task_id + x, 0, 0));
            if (!sort_base_gather) {
                count.atomic(0).fetch_add(-1u);
            }

            (*coroutine)(frame, args...);
            if (is_soa) {
                frame_buffer.write(frame_id, frame, coroutine->graph().node(0u)->output_state_members);
            } else {
                frame_buffer.write(frame_id, frame);
            }
            if (!sort_base_gather) {
                auto nxt = read_promise<uint>(frame, "coro_token") & token_mask;
                count.atomic(nxt).fetch_add(1u);
            }
        };
        ShaderOption o{};
        _gen_shader = device.compile(gen_kernel, o);
        _gen_shader.set_name("gen");
        _resume_shaders.resize(max_sub_coro);
        for (int i = 1; i < max_sub_coro; ++i) {
            Kernel1D resume_kernel = [&](BufferUInt index, BufferUInt count, Var<Container> frame_buffer, UInt n, Var<Args>... args) {
                auto x = dispatch_x();
                $if (x >= n) {
                    $return();
                };
                auto frame_id = index.read(x);
                Var<FrameType> frame;
                if (is_soa) {
                    //frame = frame_buffer.read(frame_id);
                    frame = frame_buffer.read(frame_id, coroutine->graph().node(i)->input_state_members);
                } else {
                    frame = frame_buffer.read(frame_id);
                }
                if (!sort_base_gather) {
                    count.atomic(i).fetch_add(-1u);
                }
                if (_debug) {
                    auto token = frame_buffer.read(frame_id);
                    auto check = read_promise<uint>(token, "coro_token") & token_mask;
                    /*$if (check != i) {
                        device_log("wrong launch for frame {} at kernel {} as kernel {} when dispatch {}", frame_id, check, i, dispatch_x());
                    };*/
                }
                /*if(_have_hint[i]) {
                    auto token = frame_buffer.read(frame_id);
                    device_log("dispatch:{}, index:{}, hint:{}",dispatch_x(),frame_id, read_promise<uint>(token,"coro_hint")&token_mask);
                }*/
                (*coroutine)[i](frame, args...);
                if (is_soa) {
                    frame_buffer.write(frame_id, frame, coroutine->graph().node(i)->output_state_members);
                } else {
                    frame_buffer.write(frame_id, frame);
                }
                //if (debug)
                //    device_log("resume kernel {} : id {} goto kernel {}", i, frame_id, nxt);

                if (!sort_base_gather) {
                    auto nxt = read_promise<uint>(frame, "coro_token") & token_mask;
                    $switch (nxt) {
                        for (int i = 0; i < max_sub_coro; ++i) {
                            $case (i) {
                                count.atomic(i).fetch_add(1u);
                            };
                        }
                    };
                    //count.atomic(nxt).fetch_add(1u);
                }
            };
            if (_debug) o.name = "resume" + std::to_string(i);
            _resume_shaders[i] = device.compile(resume_kernel, o);
            _resume_shaders[i].set_name("resume" + std::to_string(i));
        }
        Kernel1D _prefix_kernel = [&](BufferUInt count, BufferUInt prefix, UInt n) {
            $if (dispatch_x() == 0) {
                auto pre = def(0u);
                $for (i, 0u, _max_sub_coro) {
                    auto val = count.read(i);
                    prefix.write(i, pre);
                    pre = pre + val;
                    if (_debug) {
                        _debug_buffer->write(i, pre);
                    }
                };
            };
        };
        _count_prefix_shader = device.compile(_prefix_kernel);
        Kernel1D _collect_kernel = [&](BufferUInt index, BufferUInt prefix, Var<Container> frame_buffer, UInt n) {
            auto x = dispatch_x();
            auto frame = frame_buffer.read(x);
            auto r_id = read_promise<uint>(frame, "coro_token") & token_mask;
            auto q_id = prefix.atomic(r_id).fetch_add(1u);
            if (_debug) {
                $if (q_id >= _debug_buffer->read(r_id)) {
                    device_log("collect: indices overflow!!!! frame_id:{}>buffer_offset:{}", q_id, _debug_buffer->read(r_id));
                };
                /*
                $if (q_id == _debug_buffer->read(r_id) - 1) {
                    device_log("finish gather: kernel {}, tot :{}", r_id, _debug_buffer->read(r_id));
                };
                 */
            }
            index.write(q_id, x);
        };
        _gather_shader = device.compile(_collect_kernel);
        Kernel1D _compact_kernel_2 = [&](BufferUInt index, Var<Container> frame_buffer, UInt empty_offset, UInt n) {
            //_global_buffer->write(0u, 0u);
            auto x = dispatch_x();
            $if (empty_offset + x < n) {
                auto token = frame_buffer.read_coro_token(empty_offset + x);
                $if ((token & token_mask) != 0u) {

                    auto res = _global_buffer->atomic(0).fetch_add(1u);
                    auto slot = index.read(res);
                    if (!sort_base_gather) {
                        $while (slot >= empty_offset) {
                            res = _global_buffer->atomic(0).fetch_add(1u);
                            slot = index.read(res);
                        };
                    }
                    /*if (_debug) {
                        $if (slot >= empty_offset) {
                            device_log("compact: new slot is in empty set!!!! slot:{}>empty_offset:{}", slot, empty_offset);
                        };
                    }*/
                    if (_debug) {
                        auto empty = frame_buffer.read(slot);
                        $if ((read_promise<uint>(empty, "coro_token") & token_mask) != 0u) {
                            device_log("wrong compact for frame {} at kernel {} when dispatch {}", slot, (read_promise<uint>(empty, "coro_token") & token_mask), dispatch_x());
                        };
                    }
                    auto frame = frame_buffer.read(empty_offset + x);
                    frame_buffer.write(slot, frame);
                    Var<FrameType> empty_frame;
                    initialize_coroframe(empty_frame, def<uint3>(0, 0, 0));
                    if (is_soa) {
                        frame_buffer.write(empty_offset + x, empty_frame, {1});
                    } else {
                        frame_buffer.write(empty_offset + x, empty_frame);
                    }
                };
            };
        };
        _compact_shader = device.compile(_compact_kernel_2);
        _compact_shader.set_name("compact");

        Kernel1D _initialize_kernel = [&](BufferUInt count, Var<Container> frame_buffer, UInt n) {
            auto x = dispatch_x();
            $if (x < n) {
                auto frame = frame_buffer.read(x);
                initialize_coroframe(frame, def<uint3>(0, 0, 0));
            };
            $if (x < max_sub_coro) {
                count.write(x, ite(x == 0, _max_frame_count, 0u));
            };
        };
        Kernel1D clear = [&](BufferUInt buffer, UInt n) {
            auto x = dispatch_x();
            $if (x < n) {
                buffer.write(x, 0u);
            };
        };
        _clear_shader = device.compile(clear);
        _initialize_shader = device.compile(_initialize_kernel);
        stream << _initialize_shader(_resume_count, _frame, _max_frame_count).dispatch(_max_frame_count);
    }
    void operator()(prototype_to_coro_dispatcher_t<Args>... args, uint dispatch_size) noexcept override {
        CoroDispatcherBase<void(FrameRef, Args...)>::operator()(args..., dispatch_size);
        _dispatch_counter = 0;
        _host_empty = true;
        for (auto i = 0u; i < _max_sub_coro; i++) {
            if (i) {
                _host_count[i] = 0;
                _host_offset[i] = _max_frame_count;
            } else {
                _host_count[i] = _max_frame_count;
                _host_offset[i] = 0;
            }
        }
        _stream << _initialize_shader(_resume_count, _frame, _max_frame_count).dispatch(_max_frame_count);
    }
};

struct PersistentCoroDispatcherConfig {
    uint max_thread_count = 64_k;
    uint block_size = 128;
    uint fetch_size = 16;
    bool global = false;
    bool debug = false;
};

template<typename FrameRef, typename... Args>
class PersistentCoroDispatcher : public CoroDispatcherBase<void(FrameRef, Args...)> {

public:
    using Config = PersistentCoroDispatcherConfig;

private:
    using FrameType = std::remove_reference_t<FrameRef>;
    Shader1D<Buffer<uint>, uint, Args...> _pt_shader;
    Shader1D<Buffer<uint>> _clear_shader;
    Buffer<uint> _global;
    Buffer<FrameType> _global_frame;
    Shader1D<Buffer<FrameType>, uint> _initialize_shader;
    uint _max_sub_coro;
    uint _max_thread_count;
    uint _block_size;
    uint _global_size;
    bool _dispatched;
    bool _done;
    void _await_step(Stream &stream) noexcept;
    void _await_all(Stream &stream) noexcept;
    bool _debug;
    Stream &_stream;
public:
    bool all_dispatched() const noexcept;
    bool all_done() const noexcept;
    PersistentCoroDispatcher(Coroutine<void(FrameRef, Args...)> *coroutine,
                             Device &device, Stream &stream,
                             const PersistentCoroDispatcherConfig &config) noexcept
        : CoroDispatcherBase<void(FrameRef, Args...)>{coroutine, device},
          _max_thread_count{(config.max_thread_count+config.block_size-1)/config.block_size*config.block_size},
          _block_size{config.block_size},
          _debug{config.debug}, _stream{stream} {
        auto use_global=config.global;
        _global = device.create_buffer<uint>(1);
        auto q_fac = 1u;
        uint max_sub_coro = coroutine->suspend_count() + 1;
        auto g_fac=(uint)std::max((int)(max_sub_coro-q_fac),0);
        auto global_queue_size= config.block_size * g_fac;
        _global_size=0;
        if(use_global) {
            _global_frame = device.create_buffer<FrameType>(_max_thread_count*g_fac);
            _global_size = _max_thread_count*g_fac;
        }
        _max_sub_coro = max_sub_coro;
        _dispatched = false;
        _done = false;
        Kernel1D main_kernel = [&](BufferUInt global, UInt dispatch_size, Var<Args>... args) {
            set_block_size(config.block_size, 1, 1);
            auto shared_queue_size = config.block_size * q_fac;
            Shared<FrameType> frames{shared_queue_size};
            Shared<uint> path_id{shared_queue_size};
            Shared<uint> work_counter{max_sub_coro};
            Shared<uint> work_offset{2u};
            Shared<uint> all_token{use_global?(shared_queue_size+global_queue_size):shared_queue_size};
            Shared<uint> workload{2};
            Shared<uint> work_stat{2};//0 max_count,1 max_id
            //Shared<uint> tag_counter{use_tag_sort ? pipeline().surfaces().size() : 0};
            //Shared<uint> tag_offset{pipeline().surfaces().size()};
            $for (index, 0u, q_fac) {
                all_token[index * config.block_size + thread_x()] = 0u;
                initialize_coroframe(frames[index * config.block_size + thread_x()], def<uint3>(0, 0, 0));
            };
            $for (index, 0u, g_fac) {
                all_token[shared_queue_size+index * config.block_size + thread_x()] = 0u;
            };
            $if (thread_x() < max_sub_coro) {
                $if (thread_x() == 0) {
                    work_counter[thread_x()] = use_global?(shared_queue_size+global_queue_size):shared_queue_size;
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
            auto count = def(0u);
            auto count_limit = def<uint>(-1);
            $while ((rem_global[0] != 0u | rem_local[0] != 0u) & (count != count_limit)) {
                sync_block();//very important, synchronize for condition
                rem_local[0] = 0u;
                count += 1;
                work_stat[0] = 0;
                work_stat[1] = -1;
                sync_block();
                $if (thread_x() == config.block_size - 1) {
                    $if ((workload[0] >= workload[1]) & (rem_global[0] == 1u)) {//fetch new workload
                        workload[0] = global.atomic(0u).fetch_add(config.block_size * config.fetch_size);
                        if (_debug)
                            device_log("block {}, fetch workload: {}", block_x(), workload[0]);
                        workload[1] = min(workload[0] + config.block_size * config.fetch_size, dispatch_size);
                        $if (workload[0] >= dispatch_size) {
                            rem_global[0] = 0u;
                        };
                    };
                };
                sync_block();
                $if (thread_x() < max_sub_coro) {//get max
                    $if ((workload[0] < workload[1]) | (thread_x() != 0u)) {
                        $if (work_counter[thread_x()] != 0) {
                            rem_local[0] = 1u;
                            work_stat.atomic(0).fetch_max(work_counter[thread_x()]);
                        };
                    };
                };
                sync_block();
                $if (thread_x() < max_sub_coro) {//get argmax
                    $if ((work_stat[0] == work_counter[thread_x()]) & ((workload[0] < workload[1]) | (thread_x() != 0u))) {
                        work_stat[1] = thread_x();
                    };
                };
                sync_block();
                work_offset[0] = 0;
                work_offset[1] = 0;
                sync_block();
                if(!use_global) {
                    $for (index, 0u, q_fac) {//collect indices
                        auto frame_token = all_token[index * config.block_size + thread_x()];
                        $if (frame_token == work_stat[1]) {
                            auto id = work_offset.atomic(0).fetch_add(1u);
                            path_id[id] = index * config.block_size + thread_x();
                        };
                    };
                }else{
                    $for(index, 0u, q_fac) {//collect switch out indices
                        auto frame_token = all_token[index * config.block_size + thread_x()];
                        $if (frame_token != work_stat[1]) {
                            auto id = work_offset.atomic(0).fetch_add(1u);
                            path_id[id] = index * config.block_size + thread_x();
                        };
                    };
                    sync_block();
                    $if(shared_queue_size - work_offset[0] < config.block_size) {//no enough work
                        $for(index, 0u, g_fac) {                           //swap frames
                            auto global_id = block_x() * global_queue_size + index * config.block_size + thread_x();
                            auto g_queue_id=index * config.block_size + thread_x();
                            auto coro_token=all_token[shared_queue_size+g_queue_id];
                            $if(coro_token == work_stat[1]) {
                                auto id = work_offset.atomic(1).fetch_add(1u);
                                $if(id < work_offset[0]) {
                                    auto dst = path_id[id];
                                    auto frame_token = all_token[dst];
                                    $if(coro_token != 0u) {
                                        $if(frame_token != 0u) {
                                            auto g_state = _global_frame->read(global_id);
                                            _global_frame->write(global_id,frames[dst]);
                                            frames[dst]=g_state;
                                            all_token[shared_queue_size+g_queue_id]=frame_token;
                                            all_token[dst]=coro_token;

                                        }
                                        $else {
                                            auto g_state = _global_frame->read(global_id);
                                            frames[dst]=g_state;
                                            all_token[shared_queue_size+g_queue_id]=frame_token;
                                            all_token[dst]=coro_token;
                                        };
                                    }
                                    $else {
                                        $if(frame_token != 0u) {
                                            _global_frame->write(global_id,frames[dst]);
                                            all_token[shared_queue_size+g_queue_id]=frame_token;
                                            all_token[dst]=coro_token;
                                        };
                                    };
                                };
                            };
                        };
                    };
                }
                auto gen_st = workload[0];
                sync_block();
                auto pid = def(0u);
                if (use_global) {
                    pid = thread_x();
                } else {
                    pid = path_id[thread_x()];
                }
                auto launch_condition = def(true);
                if (!use_global) {
                    launch_condition = (thread_x() < work_offset[0]);
                }
                else{
                    launch_condition = (all_token[pid]==work_stat[1]);
                }
                $if (launch_condition) {
                    $switch (all_token[pid]) {
                        $case (0u) {
                            $if (gen_st + thread_x() < workload[1]) {
                                work_counter.atomic(0u).fetch_sub(1u);
                                auto work_id = gen_st + thread_x();
                                initialize_coroframe(frames[pid], def<uint3>(gen_st + thread_x(), 0, 0));
                                (*coroutine)(frames[pid], args...);//only work when kernel 0s are continue
                                auto nxt = read_promise<uint>(frames[pid], "coro_token") & token_mask;
                                all_token[pid]=nxt;
                                work_counter.atomic(nxt).fetch_add(1u);
                                workload.atomic(0).fetch_add(1u);
                            };
                        };
                        for (auto i = 1u; i < max_sub_coro; ++i) {
                            $case (i) {
                                work_counter.atomic(i).fetch_sub(1u);
                                (*coroutine)[i](frames[pid], args...);
                                auto nxt = read_promise<uint>(frames[pid], "coro_token") & token_mask;
                                all_token[pid]=nxt;
                                work_counter.atomic(nxt).fetch_add(1u);
                            };
                        }
                    };
                };
                sync_block();
            };
            $if (count >= count_limit) {
                device_log("block_id{},thread_id {}, loop not break! local:{}, global:{}", block_x(), thread_x(), rem_local[0], rem_global[0]);
                $if (thread_x() < max_sub_coro) {
                    device_log("work rem: id {}, size {}", thread_x(), work_counter[thread_x()]);
                };
            };
        };
        _pt_shader = device.compile(main_kernel);
        Kernel1D clear = [&](BufferUInt global) {
            global->write(dispatch_x(), 0u);
        };
        Kernel1D initialize_frame = [&](Var<Buffer<FrameType>> frame_buffer, UInt n) {
            auto x = dispatch_x();
            $if (x < n) {
                auto frame = def<FrameType>();
                initialize_coroframe(frame, def<uint3>(0, 0, 0));
                frame_buffer.write(x,frame);
            };
        };
        _clear_shader = device.compile(clear);
        _initialize_shader = device.compile(initialize_frame);
        stream << _clear_shader(_global).dispatch(1u);
    }
    void operator()(prototype_to_coro_dispatcher_t<Args>... args, uint dispatch_size) noexcept override {
        CoroDispatcherBase<void(FrameRef, Args...)>::operator()(args..., dispatch_size);
        _dispatched = false;
        _done = false;
        _stream << _clear_shader(_global).dispatch(1u);
        if(_global_size) {
            _stream << _initialize_shader(_global_frame, _global_size).dispatch(_global_size);
        }
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
    void operator()(Stream &stream) && noexcept;
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
                                                                                    prototype_to_coro_dispatcher_t<T>... prefix_args) {
    auto invoke = shader.partial_invoke(prefix_args...);
    std::apply([&invoke](prototype_to_coro_dispatcher_t<Args>... args) {
        static_cast<void>((invoke << ... << args));
    },
               _args);
    return invoke;
}
template<typename T>
void CoroAwait<T>::operator()(Stream &stream) && noexcept {
    switch (_tag) {
        case CmdTag::AWAIT_STEP: this->_dispatcher->_await_step(stream); break;
        case CmdTag::AWAIT_ALL: this->_dispatcher->_await_all(stream); break;
    }
}
/*template<typename FrameRef, typename... Args>
void WavefrontCoroDispatcher<FrameRef, Args...>::_await_step(Stream &stream) noexcept {

}*/
template<typename FrameRef, typename... Args>
void WavefrontCoroDispatcher<FrameRef, Args...>::_await_step(Stream &stream) noexcept {
    if (sort_base_gather) {
        auto host_update = [&] {
            _host_empty = true;
            auto sum = 0u;
            for (uint i = 0; i < _max_sub_coro; i++) {
                _host_count[i] = (i + 1 == _max_sub_coro ? _max_frame_count : _host_offset[i + 1]) - _host_offset[i];
                _host_empty = _host_empty && (i == 0 || _host_count[i] == 0);
            }
        };
        _sort_token.sort(stream, _temp_key[0], _resume_index, _temp_key[1], _resume_index, _max_frame_count);

        stream << _sort_temp_storage.hist_buffer.view(0, _max_sub_coro).copy_to(_host_offset.data())
               << host_update
               << synchronize();
        if (_debug)
            for (int i = 0; i < _max_sub_coro; ++i) {
                LUISA_INFO("kernel {}: total {}", i, _host_count[i]);
            }
        if (_host_count[0] > _max_frame_count * (0.5) && !all_dispatched()) {
            auto gen_count = std::min(this->_dispatch_size - this->_dispatch_counter, _host_count[0]);
            if (_debug) {
                LUISA_INFO("Gen {} new frame", gen_count);
            }
            if (_host_count[0] != _max_frame_count && use_compact) {
                stream << _clear_shader(_global_buffer, 1).dispatch(1u);
                stream << _compact_shader(_resume_index, _frame, _max_frame_count - _host_count[0], _max_frame_count).dispatch(_host_count[0]);
            }
            stream << this->template call_shader<1, Buffer<uint>, Buffer<uint>, uint, Container, uint, uint>(
                              _gen_shader, _resume_index.view(_host_offset[0], _host_count[0]), _resume_count, _max_frame_count - _host_count[0], _frame, _dispatch_counter, _max_frame_count)
                          .dispatch(gen_count);
            _dispatch_counter += gen_count;
            _host_empty = false;
        } else {
            for (uint i = 1; i < _max_sub_coro; i++) {
                if (_debug)
                    LUISA_INFO("launch kernel {} for count {}", i, _host_count[i]);
                if (_host_count[i] > 0) {
                    if (_have_hint[i]) {
                        BufferView<uint> _index[2] = {_resume_index.view(_host_offset[i], _host_count[i]), _temp_index.view(_host_offset[i], _host_count[i])};
                        BufferView<uint> _key[2] = {_temp_key[1].view(_host_offset[i], _host_count[i]), _temp_key[0].view(_host_offset[i], _host_count[i])};
                        uint out = _sort_hint.sort_switch(stream, _key, _index, _host_count[i],_resume_index.view(_host_offset[i], _host_count[i]));
                        stream << this->template call_shader<1, Buffer<uint>, Buffer<uint>, Container, uint>(_resume_shaders[i], _index[out],
                                                                                                             _resume_count, _frame, _max_frame_count)
                                      .dispatch(_host_count[i]);
                    } else {

                        stream << this->template call_shader<1, Buffer<uint>, Buffer<uint>, Container, uint>(_resume_shaders[i], _resume_index.view(_host_offset[i], _host_count[i]),
                                                                                                             _resume_count, _frame, _max_frame_count)
                                      .dispatch(_host_count[i]);
                    }
                }
            }
        }
        stream << synchronize();
    } else {
        if (_debug)
            for (int i = 0; i < _max_sub_coro; ++i) {
                LUISA_INFO("kernel {}: total {}", i, _host_count[i]);
            }
        stream << _count_prefix_shader(_resume_count, _resume_offset, _max_sub_coro).dispatch(1u);
        stream << _gather_shader(_resume_index, _resume_offset, _frame, _max_frame_count).dispatch(_max_frame_count);
        if (_host_count[0] > _max_frame_count / 2 && !all_dispatched()) {
            auto gen_count = std::min(this->_dispatch_size - this->_dispatch_counter, _host_count[0]);
            if (_debug) {
                LUISA_INFO("Gen {} new frame", gen_count);
            }
            if (_host_count[0] != _max_frame_count && use_compact) {
                stream << _clear_shader(_global_buffer, 1).dispatch(1u);
                stream
                    << _compact_shader(_resume_index.view(_host_offset[0], _host_count[0]), _frame, _max_frame_count - _host_count[0], _max_frame_count).dispatch(_host_count[0]);
            }
            stream << this->template call_shader<1, Buffer<uint>, Buffer<uint>, uint, Container, uint, uint>(
                              _gen_shader, _resume_index.view(_host_offset[0], _host_count[0]), _resume_count, _max_frame_count - _host_count[0], _frame, _dispatch_counter, _max_frame_count)
                          .dispatch(gen_count);
            _dispatch_counter += gen_count;
            _host_empty = false;
        } else {
            for (uint i = 1; i < _max_sub_coro; i++) {
                if (_debug)
                    LUISA_INFO("launch kernel {} for count {}", i, _host_count[i]);
                if (_host_count[i] > 0) {
                    if (_have_hint[i]) {
                        BufferView<uint> _index[2] = {_resume_index.view(_host_offset[i], _host_count[i]), _temp_index.view(_host_offset[i], _host_count[i])};
                        BufferView<uint> _key[2] = {_temp_key[0].view(_host_offset[i], _host_count[i]), _temp_key[1].view(_host_offset[i], _host_count[i])};
                        uint out = _sort_hint.sort_switch(stream, _key, _index, _host_count[i],_resume_index.view(_host_offset[i], _host_count[i]));
                        stream << this->template call_shader<1, Buffer<uint>, Buffer<uint>, Container, uint>(_resume_shaders[i], _index[out],
                                                                                                             _resume_count, _frame, _max_frame_count)
                                      .dispatch(_host_count[i]);
                    } else {
                        stream << this->template call_shader<1, Buffer<uint>, Buffer<uint>, Container, uint>(_resume_shaders[i], _resume_index.view(_host_offset[i], _host_count[i]),
                                                                                                             _resume_count, _frame, _max_frame_count)
                                      .dispatch(_host_count[i]);
                    }
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
        stream << synchronize();
    }
}
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
    _dispatched = true;
    stream << this->template call_shader<1, Buffer<uint>, uint>(_pt_shader, _global, this->_dispatch_size).dispatch(_max_thread_count)
           << [&] { _done = true; };
    stream << synchronize();
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
