//
// Created by Mike Smith on 2023/5/18.
//

#include <luisa/core/logging.h>
#include <luisa/runtime/command_buffer.h>

namespace luisa::compute {

CommandBuffer::CommandBuffer(Stream *stream) noexcept
    : _stream{stream} {}

CommandBuffer::~CommandBuffer() noexcept {
    LUISA_ASSERT(_list.empty(),
                 "Command buffer not empty when destroyed. "
                 "Did you forget to commit?");
}

}// namespace luisa::compute
