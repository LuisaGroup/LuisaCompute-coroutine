//
// Created by Mike Smith on 2022/2/13.
//

#pragma once

#include <mutex>
#include <thread>

#include <ast/function_builder.h>
#include <runtime/buffer.h>
#include <runtime/event.h>
#include <dsl/operators.h>
#include <dsl/expr.h>
#include <dsl/var.h>

namespace luisa::compute {

class Device;
class Stream;

class Printer {

public:
    struct Descriptor {
        enum struct Tag {
            INT,
            UINT,
            FLOAT,
            BOOL,
            STRING
        };
        luisa::vector<Tag> value_tags;

        [[nodiscard]] auto operator==(const Descriptor &rhs) const noexcept {
            for (auto i = 0u; i < value_tags.size(); i++) {
                if (value_tags[i] != rhs.value_tags[i]) {
                    return false;
                }
            }
            return true;
        }
    };

    struct DescriptorHash {
        [[nodiscard]] auto operator()(const Descriptor &desc) const noexcept {
            return luisa::detail::xxh3_hash64(
                desc.value_tags.data(),
                desc.value_tags.size() * sizeof(Descriptor),
                Hash64::default_seed);
        }
    };

private:
    Buffer<uint> _buffer;// count & records (desc_id, arg0, arg1, ...)
    luisa::vector<uint> _host_buffer;
    luisa::unordered_map<Descriptor, uint, DescriptorHash, std::equal_to<>> _desc_id;
    luisa::unordered_map<luisa::string, uint, Hash64, std::equal_to<>> _string_id;
    luisa::vector<Descriptor> _descriptors;
    luisa::vector<luisa::string_view> _strings;
    luisa::string _scratch;
    uint _uid{};
    bool _reset_called{false};

public:
    explicit Printer(Device &device, size_t capacity = 16_mb) noexcept;
    void reset(Stream &stream) noexcept;
    [[nodiscard]] luisa::string_view retrieve(Stream &stream) noexcept;

    template<typename... Args>
    void log(Args &&...args) noexcept;

    template<typename... Args>
    void log_with_location(Args &&...args) noexcept {
        log(luisa::format("[#{}:", _uid++),
            dispatch_x(), ",",
            dispatch_y(), ",",
            dispatch_z(), "] ",
            std::forward<Args>(args)...);
    }
};

template<typename... Args>
void Printer::log(Args &&...args) noexcept {
    auto count = 1u /* desc_id */ + static_cast<uint>(sizeof...(args));
    auto size = static_cast<uint>(_buffer.size() - 1u);
    auto offset = _buffer.atomic(size).fetch_add(count);
    auto process_arg = [&]<typename Arg>(Arg &&arg) noexcept {
        offset = offset + 1u;
        using T = expr_value_t<Arg>;
        if constexpr (std::is_same_v<T, bool>) {
            _buffer.write(offset, cast<uint>(std::forward<Arg>(arg)));
            return Descriptor::Tag::BOOL;
        } else if constexpr (std::is_same_v<T, int>) {
            _buffer.write(offset, cast<uint>(std::forward<Arg>(arg)));
            return Descriptor::Tag::INT;
        } else if constexpr (std::is_same_v<T, uint>) {
            _buffer.write(offset, cast<uint>(std::forward<Arg>(arg)));
            return Descriptor::Tag::UINT;
        } else if constexpr (std::is_same_v<T, float>) {
            _buffer.write(offset, as<uint>(std::forward<Arg>(arg)));
            return Descriptor::Tag::FLOAT;
        } else {
            luisa::string s{std::forward<Arg>(arg)};
            auto [iter, not_present] = _string_id.try_emplace(
                std::move(s), static_cast<uint>(_strings.size()));
            if (not_present) { _strings.emplace_back(iter->first); }
            _buffer.write(offset, iter->second);
            return Descriptor::Tag::STRING;
        }
    };
    auto index = offset;
    Descriptor desc;
    if_(offset + count <= size, [&] {
        desc.value_tags = {
            process_arg(std::forward<Args>(args))...};
    });
    if_(index < size, [&] {
        auto [iter, not_present] = _desc_id.try_emplace(
            std::move(desc), static_cast<uint>(_descriptors.size()));
        if (not_present) { _descriptors.emplace_back(iter->first); }
        _buffer.write(index, iter->second);
    });
}

}// namespace luisa::compute