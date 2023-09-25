#pragma once

#include <luisa/core/basic_types.h>
#include <luisa/core/stl/vector.h>

namespace luisa::compute::inline coro {

struct CoroTransition {
    uint destination;
    luisa::vector<uint> input_state_members;
    luisa::vector<uint> output_state_members;
};

}// namespace luisa::compute::inline coro
