#pragma once

#include <runtime/shader.h>
#include <runtime/raster/raster_state.h>
#include <runtime/raster/depth_buffer.h>

namespace luisa::compute {

class RasterMesh;
class Accel;
class BindlessArray;

namespace detail {

template<typename T>
struct PixelDst : public std::false_type {};

template<typename T>
struct PixelDst<Image<T>> : public std::true_type {
    static ShaderDispatchCommandBase::Argument::Texture get(Image<T> const &v) noexcept {
        return {v.handle(), 0};
    }
};

template<typename T>
struct PixelDst<ImageView<T>> : public std::true_type {
    static ShaderDispatchCommandBase::Argument::Texture get(ImageView<T> const &v) noexcept {
        return {v.handle(), v.level()};
    }
};

template<typename T, typename... Args>
static constexpr bool LegalDst() noexcept {
    constexpr bool r = PixelDst<T>::value;
    if constexpr (sizeof...(Args) == 0) {
        return r;
    } else if constexpr (!r) {
        return false;
    } else {
        return LegalDst<Args...>();
    }
}

}// namespace detail

class LC_RUNTIME_API RasterShaderInvoke {

private:
    RasterDispatchCmdEncoder _command;
    luisa::span<const Function::Binding> _vertex_bindings;
    luisa::span<const Function::Binding> _pixel_bindings;

public:
    explicit RasterShaderInvoke(
        size_t arg_size,
        size_t uniform_size,
        uint64_t handle,
        luisa::span<const Function::Binding> vertex_bindings,
        luisa::span<const Function::Binding> pixel_bindings) noexcept
        : _command{handle, arg_size, uniform_size, vertex_bindings, pixel_bindings} {
    }
    RasterShaderInvoke(RasterShaderInvoke &&) noexcept = default;
    RasterShaderInvoke(const RasterShaderInvoke &) noexcept = delete;
    RasterShaderInvoke &operator=(RasterShaderInvoke &&) noexcept = default;
    RasterShaderInvoke &operator=(const RasterShaderInvoke &) noexcept = delete;

    template<typename T>
    RasterShaderInvoke &operator<<(BufferView<T> buffer) noexcept {
        _command.encode_buffer(buffer.handle(), buffer.offset_bytes(), buffer.size_bytes());
        return *this;
    }

    template<typename T>
    RasterShaderInvoke &operator<<(ImageView<T> image) noexcept {
        _command.encode_texture(image.handle(), image.level());
        return *this;
    }

    template<typename T>
    RasterShaderInvoke &operator<<(VolumeView<T> volume) noexcept {
        _command.encode_texture(volume.handle(), volume.level());
        return *this;
    }

    template<typename T>
    RasterShaderInvoke &operator<<(const Buffer<T> &buffer) noexcept {
        return *this << buffer.view();
    }

    template<typename T>
    RasterShaderInvoke &operator<<(const Image<T> &image) noexcept {
        return *this << image.view();
    }

    template<typename T>
    RasterShaderInvoke &operator<<(const Volume<T> &volume) noexcept {
        return *this << volume.view();
    }

    template<typename T>
    RasterShaderInvoke &operator<<(T data) noexcept {
        _command.encode_uniform(&data, sizeof(T));
        return *this;
    }

    // see definition in rtx/accel.cpp
    RasterShaderInvoke &operator<<(const Accel &accel) noexcept;
    // see definition in runtime/bindless_array.cpp
    RasterShaderInvoke &operator<<(const BindlessArray &array) noexcept;
#ifndef NDEBUG
    MeshFormat const *_mesh_format;
    RasterState const *_raster_state;
    luisa::span<PixelFormat const> _rtv_format;
    DepthFormat _dsv_format;
    void check_dst(luisa::span<PixelFormat const> rt_formats, DepthBuffer const *depth) noexcept;
    void check_scene(luisa::span<RasterMesh const> scene) noexcept;
#endif
    template<typename... Rtv>
        requires(sizeof...(Rtv) == 0 || detail::LegalDst<Rtv...>())
    [[nodiscard]] auto draw(luisa::vector<RasterMesh> &&scene, Viewport viewport, DepthBuffer const *dsv, Rtv const &...rtv) &&noexcept {
        if (dsv) {
            _command.set_dsv_tex(ShaderDispatchCommandBase::Argument::Texture{dsv->handle(), 0});
        } else {
            _command.set_dsv_tex(ShaderDispatchCommandBase::Argument::Texture{~0ull, 0});
        }
        if constexpr (sizeof...(Rtv) > 0) {
            auto tex_args = {detail::PixelDst<std::remove_cvref_t<Rtv>>::get(rtv)...};
            _command.set_rtv_texs(tex_args);
#ifndef NDEBUG
            auto rtv_formats = {rtv.format()...};
            check_dst({rtv_formats.begin(), rtv_formats.size()}, dsv);
#endif
        }
#ifndef NDEBUG
        check_scene(scene);
#endif
        _command.set_scene(std::move(scene));
        _command.set_viewport(viewport);
        return std::move(_command).build();
    }
};

namespace detail {
LC_RUNTIME_API void rastershader_check_rtv_format(luisa::span<const PixelFormat> rtv_format) noexcept;
LC_RUNTIME_API void rastershader_check_vertex_func(Function func) noexcept;
LC_RUNTIME_API void rastershader_check_pixel_func(Function func) noexcept;
}// namespace detail

template<typename... Args>
class RasterShader : public Resource {

private:
    friend class Device;
    luisa::vector<Function::Binding> _vertex_bindings;
    luisa::vector<Function::Binding> _pixel_bindings;
    size_t _uniform_size;
#ifndef NDEBUG
    MeshFormat _mesh_format;
    RasterState _raster_state;
    luisa::vector<PixelFormat> _rtv_format;
    DepthFormat _dsv_format;
#endif
    // JIT Shader
    // clang-format off

    RasterShader(
        DeviceInterface *device,
        const MeshFormat &mesh_format,
        const RasterState &raster_state,
        luisa::span<PixelFormat const> rtv_format,
        DepthFormat dsv_format,
        luisa::shared_ptr<const detail::FunctionBuilder> vert,
        luisa::shared_ptr<const detail::FunctionBuilder> pixel,
        luisa::string_view name,
        bool enable_debug_info,
        bool enable_fast_math) noexcept
        : Resource(
              device,
              Tag::RASTER_SHADER,
              device->create_raster_shader(
                  mesh_format,
                  raster_state,
                  rtv_format,
                  dsv_format,
                  Function(vert.get()),
                  Function(pixel.get()),
                  ShaderOption{
                    .enable_cache = true,
                    .enable_fast_math = enable_fast_math,
                    .enable_debug_info = enable_debug_info,
                    .name = name})),
         _uniform_size{ShaderDispatchCmdEncoder::compute_uniform_size(
              detail::shader_argument_types<Args...>())}
#ifndef NDEBUG
        ,_mesh_format(mesh_format),
        _raster_state(raster_state),
        _dsv_format(dsv_format)
#endif
        {
#ifndef NDEBUG
            _rtv_format.resize(rtv_format.size());
            memcpy(_rtv_format.data(), rtv_format.data(), rtv_format.size_bytes());
            detail::rastershader_check_rtv_format(_rtv_format);
            detail::rastershader_check_vertex_func(Function{vert.get()});
            detail::rastershader_check_pixel_func(Function{pixel.get()});
#endif
            auto copy_vec = [](auto&& src, auto&& dst){
                src.push_back_uninitialized(dst.size());
                std::memcpy(src.data(), dst.data(), dst.size_bytes());
            };
            auto vert_bindings = vert->argument_bindings();
            auto pixel_bindings = pixel->argument_bindings();
            copy_vec(_vertex_bindings, vert_bindings);
            copy_vec(_pixel_bindings, pixel_bindings);
#ifndef NDEBUG
        
#endif
        }

    RasterShader(DeviceInterface *device,
                 const MeshFormat &mesh_format,
                 const RasterState &raster_state,
                 luisa::span<PixelFormat const> rtv_format,
                 DepthFormat dsv_format,
                 luisa::shared_ptr<const detail::FunctionBuilder> vert,
                 luisa::shared_ptr<const detail::FunctionBuilder> pixel,
                 bool enable_cache,
                 bool enable_debug_info,
                 bool enable_fast_math)noexcept
        : Resource(
              device,
              Tag::RASTER_SHADER,
              device->create_raster_shader(
                  mesh_format,
                  raster_state,
                  rtv_format,
                  dsv_format,
                  Function(vert.get()),
                  Function(pixel.get()),
                  ShaderOption{
                    .enable_cache = enable_cache,
                    .enable_fast_math = enable_fast_math,
                    .enable_debug_info = enable_debug_info})),
         _uniform_size{ShaderDispatchCmdEncoder::compute_uniform_size(
                detail::shader_argument_types<Args...>())}
#ifndef NDEBUG
        ,_mesh_format(mesh_format),
        _raster_state(raster_state),
        _dsv_format(dsv_format)
#endif
        {
#ifndef NDEBUG
            _rtv_format.resize(rtv_format.size());
            memcpy(_rtv_format.data(), rtv_format.data(), rtv_format.size_bytes());
            detail::rastershader_check_rtv_format(_rtv_format);
            detail::rastershader_check_vertex_func(Function{vert.get()});
            detail::rastershader_check_pixel_func(Function{pixel.get()});
#endif
            auto copy_vec = [](auto&& src, auto&& dst){
                src.push_back_uninitialized(dst.size());
                std::memcpy(src.data(), dst.data(), dst.size_bytes());
            };
            auto vert_bindings = vert->argument_bindings();
            auto pixel_bindings = pixel->argument_bindings();
            copy_vec(_vertex_bindings, vert_bindings);
            copy_vec(_pixel_bindings, pixel_bindings);
        }
    // AOT Shader
    RasterShader(
        DeviceInterface *device,
        const MeshFormat &mesh_format,
        const RasterState &raster_state,
        luisa::span<PixelFormat const> rtv_format,
        DepthFormat dsv_format,
        string_view file_path)noexcept
        : Resource(
              device,
              Tag::RASTER_SHADER,
              // TODO
              device->load_raster_shader(
                mesh_format,
                raster_state,
                rtv_format,
                dsv_format,
                detail::shader_argument_types<Args...>(),
                file_path)),
            _uniform_size{ShaderDispatchCmdEncoder::compute_uniform_size(
                detail::shader_argument_types<Args...>())}
#ifndef NDEBUG
        ,_mesh_format(mesh_format),
        _raster_state(raster_state),
        _dsv_format(dsv_format)
#endif
        {
#ifndef NDEBUG
            _rtv_format.resize(rtv_format.size());
            memcpy(_rtv_format.data(), rtv_format.data(), rtv_format.size_bytes());
            detail::rastershader_check_rtv_format(_rtv_format);
#endif
        }
    // clang-format on

public:
    RasterShader() noexcept = default;
    RasterShader(RasterShader &&) noexcept = default;
    RasterShader(RasterShader const &) noexcept = delete;
    RasterShader &operator=(RasterShader &&) noexcept = default;
    RasterShader &operator=(RasterShader const &) noexcept = delete;
    using Resource::operator bool;
    [[nodiscard]] auto operator()(detail::prototype_to_shader_invocation_t<Args>... args) const noexcept {
        size_t arg_size;
        if (_vertex_bindings.empty() || _pixel_bindings.empty()) {
            arg_size = sizeof...(Args);
        } else {
            arg_size = (_vertex_bindings.size() + _pixel_bindings.size() - 2);
        }
        RasterShaderInvoke invoke(
            arg_size,
            _uniform_size,
            handle(),
            _vertex_bindings,
            _pixel_bindings);
#ifndef NDEBUG
        invoke._raster_state = &_raster_state;
        invoke._mesh_format = &_mesh_format;
        invoke._dsv_format = _dsv_format;
        invoke._rtv_format = _rtv_format;
#endif
        return std::move((invoke << ... << args));
    }
};

}// namespace luisa::compute