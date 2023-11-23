
/// radix sort adapted from cub, the onesweep version
/// reference: [Onesweep: A Faster Least Significant Digit Radix Sort for GPUs]
/// https://arxiv.org/abs/2206.01784
#include <luisa/luisa-compute.h>
#include <luisa/coro/coro_dispatcher.h>
using namespace luisa;
using namespace luisa::compute;

const uint BIT = 7;
const uint DIGIT = 1 << BIT;
const uint HIST_BLOCK_SIZE = 128;
const uint SM_COUNT = 256;
const uint ONESWEEP_BLOCK_SIZE = 256;
const uint ONESWEEP_ITEM_COUNT = 32;
const uint WARP_LOG = 5;
const uint WARP_SIZE = 1 << WARP_LOG;
const uint WARP_MASK = WARP_SIZE - 1;
const uint ALL_MASK = 0xffff'ffff;
static_assert(ONESWEEP_BLOCK_SIZE % WARP_SIZE == 0);
Buffer<uint> hist_buffer;
Buffer<uint> bin_buffer;
Buffer<uint> launch_count;
luisa::vector<uint> bit_split;
Buffer<uint> rank;
Buffer<uint> key_out;
Buffer<uint> guard;
const uint BIN_LOCAL_MASK = 0x4000'0000;
const uint BIN_GLOBAL_MASK = 0x8000'0000;
const uint BIN_VAL_MASK = 0x3fff'ffff;
inline uint ceil_div(uint x, uint y) {
    return (x + y - 1) / y;
}
int main(int argc, char *argv[]) {
    log_level_verbose();
    auto context = Context{argv[0]};
    if (argc <= 1) {
        LUISA_INFO("Usage: {} <backend>. <backend>: cuda, dx, ispc, metal", argv[0]);
        exit(1);
    }
    auto device = context.create_device(argv[1]);
    auto stream = device.create_stream(StreamTag::COMPUTE);
    constexpr auto n = 1024 * 1024 * 32u;
    bool debug = 0u;

    srand(288282);
    auto x_buffer = device.create_buffer<uint>(n);
    auto x_vec = luisa::vector<uint>(n, 0u);
    auto x_rank = luisa::vector<uint>(n, 0u);
    auto x_bin = luisa::vector<uint>(DIGIT, 0u);
    for (int i = 0; i < n; ++i) {
        auto val = rand();
        x_vec[i] = val;
        x_rank[i] = -1;
        //LUISA_INFO("x[{}]:{}",i,val);
    }
    stream << x_buffer.copy_from(x_vec.data());
    stream << synchronize();
    eastl::sort(x_vec.begin(), x_vec.end());
    auto low_bit = 0u;
    auto high_bit = 31;
    for (int i = low_bit; i <= high_bit; i += BIT) {
        bit_split.push_back(i);
    }
    uint HIST_GROUP = bit_split.size();
    hist_buffer = device.create_buffer<uint>(HIST_GROUP * DIGIT);
    launch_count = device.create_buffer<uint>(1u);
    bin_buffer = device.create_buffer<uint>(ceil_div(n, ONESWEEP_BLOCK_SIZE * ONESWEEP_ITEM_COUNT) * DIGIT);
    rank = device.create_buffer<uint>(n);
    key_out = device.create_buffer<uint>(n);
    guard = device.create_buffer<uint>(n);
    Callable get_key = [&](BufferUInt key, UInt i) {
        auto ret = def<uint>(0);
        $if (i < n) {
            ret = key.read(i);
        }
        $else {
            ret = 0xffff'ffff;
        };
        return ret;
    };
    Kernel1D get_hist = [&](BufferUInt key_buffer, BufferUInt hist_buffer, UInt item_count, UInt n) {
        set_block_size(HIST_BLOCK_SIZE);
        Shared<uint> local_hist{DIGIT * HIST_GROUP};
        $for (i, 0u, ceil_div(DIGIT * HIST_GROUP, HIST_BLOCK_SIZE)) {
            $if (i * HIST_BLOCK_SIZE + thread_x() < DIGIT * HIST_GROUP) {
                local_hist[i * HIST_BLOCK_SIZE + thread_x()] = 0;
            };
        };
        sync_block();
        $for (i, 0u, item_count) {
            auto id = thread_x() + i * HIST_BLOCK_SIZE + item_count * HIST_BLOCK_SIZE * block_x();
            for (auto j = 0u; j < HIST_GROUP; ++j) {
                auto index = (get_key(key_buffer, id) >> bit_split[j]) & ((1 << BIT) - 1);
                local_hist.atomic(index + j * DIGIT).fetch_add(1u);
            }
        };
        sync_block();
        $for (i, 0u, ceil_div(DIGIT * HIST_GROUP, HIST_BLOCK_SIZE)) {
            $if (i * HIST_BLOCK_SIZE + thread_x() < DIGIT * HIST_GROUP) {
                auto cur_dig = i * HIST_BLOCK_SIZE + thread_x();
                hist_buffer.atomic(cur_dig).fetch_add(local_hist[cur_dig]);
            };
        };
    };
    Kernel1D get_accum = [&](BufferUInt hist_buffer) {
        set_block_size(32);
        $if (thread_x() == 0) {
            auto prefix = def<uint>(0u);
            $for (i, 0u, DIGIT) {

                auto cur_dig = i + block_x() * DIGIT;
                auto val = hist_buffer.read(cur_dig);
                hist_buffer.write(cur_dig, prefix);
                prefix += val;
                if (debug) {
                    device_log("hist_buffer[{},{}]={}to{}", block_x(), i, val, prefix);
                }
            };
        };
    };
    ExternalCallable<void()> thread_fence{"([] { __threadfence(); })"};
    ExternalCallable<uint(uint)> match_any{"([](unsigned int x) { return __match_any_sync(0xffff'ffff,x); })"};
    uint ONESWEEP_TOT_BLOCK = ceil_div(n, ONESWEEP_BLOCK_SIZE * ONESWEEP_ITEM_COUNT);
    Kernel1D onesweep = [&](BufferUInt key_buffer, BufferUInt out_buffer, BufferUInt launch_counter, BufferUInt hist_buffer,
                            BufferUInt bin, UInt low_bit, UInt n) {
        /// break_up:
        /// block(0,(warp(id=0,item=0)(0, 1, 2, ..., WARP_SIZE) warp(0,1) warp(0,2) ... | warp(1,0) ...| warp(2,0) ... | ...) block(1) block(2) ...
        set_block_size(ONESWEEP_BLOCK_SIZE);
        Shared<uint> block_id{1};
        $if (thread_x() == 0) {
            block_id[0] = launch_counter.atomic(0u).fetch_add(1u);
        };
        Shared<uint> warp_prefix{ONESWEEP_BLOCK_SIZE / WARP_SIZE * DIGIT};
        Shared<uint> block_bin{DIGIT};
        Shared<uint> local_rank{ONESWEEP_BLOCK_SIZE * ONESWEEP_ITEM_COUNT};
        warp_prefix[thread_x()] = 0;
        $for (i, 0u, ceil_div(ONESWEEP_BLOCK_SIZE / WARP_SIZE * DIGIT, ONESWEEP_BLOCK_SIZE)) {
            $if (i * ONESWEEP_BLOCK_SIZE + thread_x() < ONESWEEP_BLOCK_SIZE / WARP_SIZE * DIGIT) {
                warp_prefix[i * ONESWEEP_BLOCK_SIZE + thread_x()] = 0u;
            };
        };
        sync_block();
        auto bid = block_id[0];
        ///get warp level prefix and rank(according to single digit)
        auto lane_id = thread_x() & WARP_MASK;
        auto warp_id = thread_x() >> WARP_LOG;
        auto block_offset = bid * ONESWEEP_ITEM_COUNT * ONESWEEP_BLOCK_SIZE;
        auto warp_offset = ONESWEEP_ITEM_COUNT * WARP_SIZE * warp_id;
        $for (i, 0u, ONESWEEP_ITEM_COUNT) {
            auto read_pos = block_offset + warp_offset + i * WARP_SIZE + lane_id;
            auto key = get_key(key_buffer, read_pos);
            key = (key >> low_bit) & ((1 << BIT) - 1);
            auto prefix = def<uint>(0u);
            auto total = def<uint>(0u);
            ///general case function
            /*for(auto j=0u;j<WARP_SIZE;++j){
                auto x=warp_read_lane(key,j);
                auto now_pre=warp_prefix_count_bits(key==x);
                auto now_tot= warp_active_count_bits(key==x);
                $if(j==lane_id){
                    prefix=now_pre;
                    total=now_tot;
                };
            }*/
            auto matched = match_any(key);
            prefix = popcount(matched & ((1u << lane_id) - 1));
            total = popcount(matched);
            auto warp_pre = warp_prefix[warp_id * DIGIT + key];
            $if (prefix == 0u) {
                warp_prefix[warp_id * DIGIT + key] = warp_pre + total;
            };
            $if (read_pos < n) {
                local_rank[warp_offset + i * WARP_SIZE + lane_id] = prefix + warp_pre;
            };
        };
        sync_block();
        //get block level prefix, the calculate global offset
        $for (i, 0u, ceil_div(DIGIT, ONESWEEP_BLOCK_SIZE)) {
            auto cur_dig = i * ONESWEEP_BLOCK_SIZE + thread_x();
            $if (cur_dig < DIGIT) {
                auto digit_pre = def<uint>(0u);
                $for (cur_warp, 0u, ceil_div(ONESWEEP_BLOCK_SIZE, WARP_SIZE)) {
                    auto warp_pre = warp_prefix[cur_warp * DIGIT + cur_dig];
                    warp_prefix[cur_warp * DIGIT + cur_dig] = digit_pre;
                    digit_pre += warp_pre;
                };
                bin.write(bid * DIGIT + cur_dig, digit_pre | BIN_LOCAL_MASK);
                auto ptr = def<int>(bid - 1);
                auto global_pre = def<uint>(0u);
                $while (ptr >= 0) {
                    auto read_v = def<uint>(0u);
                    $while ((read_v == 0u)) {
                        read_v = bin.read(ptr * DIGIT + cur_dig);
                        thread_fence();
                    };
                    global_pre += (read_v & BIN_VAL_MASK);
                    $if ((read_v & BIN_GLOBAL_MASK) != 0) {
                        $break;
                    };
                    ptr -= 1;
                };
                bin.write(bid * DIGIT + cur_dig, (global_pre + digit_pre) | BIN_GLOBAL_MASK);
                block_bin[cur_dig] = global_pre;
            };
        };
        sync_block();
        //get final rank
        $for (i, 0u, ONESWEEP_ITEM_COUNT) {
            auto read_pos = block_offset + warp_offset + i * WARP_SIZE + lane_id;
            $if (read_pos < n) {
                UInt warp_rank = local_rank[warp_offset + i * WARP_SIZE + lane_id];
                auto val = get_key(key_buffer, read_pos);
                auto key = (val >> low_bit) & ((1 << BIT) - 1);
                warp_rank += warp_prefix[warp_id * DIGIT + key];//offset between warp in a block
                warp_rank += block_bin[key];                    //offset between block in global
                warp_rank += hist_buffer.read(key);             //offset between digits
                if (debug) {
                    $if (warp_rank >= n) {
                        device_log("bid:{}, warp_id:{}, lane_id:{}:rank out of bounds!", bid, warp_id, lane_id);
                        $continue;
                    };
                    out_buffer.write(warp_rank, val);
                } else {
                    out_buffer.write(warp_rank, val);
                }
            };
        };
    };
    Kernel1D clear = [](BufferUInt v) {
        auto x = dispatch_x();
        v.write(x, 0u);
    };
    auto clear_shader = device.compile(clear);
    auto hist_shader = device.compile(get_hist);
    auto accum_shader = device.compile(get_accum);
    auto onesweep_shader = device.compile(onesweep);
    auto thread_count = HIST_BLOCK_SIZE * SM_COUNT;
    if (n >= (1 << 30)) {
        LUISA_ERROR("unsupported array size!");
    }
    Clock clock;
    stream << synchronize();
    clock.tic();
    Buffer<uint> *x_in = &x_buffer;
    Buffer<uint> *x_out = &key_out;
    uint test_case = 1000;
    for (int i = 1; i <= test_case; ++i) {
        stream << clear_shader(hist_buffer).dispatch(HIST_GROUP * DIGIT);
        if (thread_count >= n) {
            stream << hist_shader(x_buffer, hist_buffer, 1, n).dispatch(ceil_div(n, HIST_BLOCK_SIZE) * HIST_BLOCK_SIZE);
        } else {
            stream << hist_shader(x_buffer, hist_buffer, ceil_div(n, SM_COUNT * HIST_BLOCK_SIZE), n).dispatch(SM_COUNT * HIST_BLOCK_SIZE);
        }
        stream << accum_shader(hist_buffer).dispatch(HIST_GROUP * 32);
        for (int i = 0; i < HIST_GROUP; ++i) {
            stream << clear_shader(launch_count).dispatch(1u);
            stream << clear_shader(bin_buffer).dispatch(ceil_div(n, ONESWEEP_BLOCK_SIZE * ONESWEEP_ITEM_COUNT) * DIGIT);
            stream << onesweep_shader(*x_in, *x_out, launch_count, hist_buffer.view(i * DIGIT, DIGIT), bin_buffer, bit_split[i], n).dispatch(ceil_div(n, ONESWEEP_BLOCK_SIZE * ONESWEEP_ITEM_COUNT) * ONESWEEP_BLOCK_SIZE);
            std::swap(x_in, x_out);
        }
        stream << synchronize();
    }
    auto gpu_time = clock.toc();
    LUISA_INFO("sort total {} token for {} times with bit [{},{}] used {} ms. Performance: {} G token/s", n, test_case, low_bit, high_bit, gpu_time, (double)n * test_case / gpu_time * 1000 / 1024 / 1024 / 1024);
    stream << x_in->copy_to(x_rank.data());
    stream << synchronize();
    int pre = 0;
    /*
    for(int i=0;i<n;++i){
        printf("%u ",x_rank[i]);
    }
    printf("\n");
    for(int i=0;i<n;++i){
printf("%u ",x_vec[i]);
    }
    printf("\n");
    */
    for (int i = 0; i < n; ++i) {
        LUISA_ASSERT(x_rank[i] == x_vec[i], "not same as eastl::sort! at {},std:{},our:{}", i, x_vec[i], x_rank[i]);
        LUISA_ASSERT(pre <= x_rank[i], "sort failed at:{},pre:{},cur:{}", i, pre, x_rank[i]);
        pre = x_rank[i];
    }
    LUISA_INFO("verified, same as eastl::sort!");
}