#include <luisa/core/logging.h>
#include <luisa/dsl/func.h>
#include <luisa/ir/ast2ir.h>

namespace luisa::compute::detail {

void CallableInvoke::_error_too_many_arguments() noexcept {
    LUISA_ERROR_WITH_LOCATION("Too many arguments for callable.");
}

#ifdef LUISA_ENABLE_IR
template<typename M>
void perform_autodiff_transform(M *m) noexcept {
    auto autodiff_pipeline = ir::luisa_compute_ir_transform_pipeline_new();
    ir::luisa_compute_ir_transform_pipeline_add_transform(autodiff_pipeline, "autodiff");
    auto converted_module = ir::luisa_compute_ir_transform_pipeline_transform_module(autodiff_pipeline, m->module);
    ir::luisa_compute_ir_transform_pipeline_destroy(autodiff_pipeline);
    m->module = converted_module;
}

void perform_coroutine_transform(ir::CallableModule *m) noexcept {
    auto coroutine_pipeline = ir::luisa_compute_ir_transform_pipeline_new();
    ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "canonicalize_control_flow");
    ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "demote_locals");
    // ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "split_coro");
    ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "extract_loop_cond");
    ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "split_coro");
    auto converted_module = ir::luisa_compute_ir_transform_pipeline_transform_callable(coroutine_pipeline, *m);
    ir::luisa_compute_ir_transform_pipeline_destroy(coroutine_pipeline);
    *m = converted_module;
}
#endif

luisa::shared_ptr<const FunctionBuilder>
transform_function(Function function) noexcept {

    // Note: we only consider the direct builtin callables here since the indirect
    //       ones should have been transformed by the called function.
    if (function.direct_builtin_callables().uses_autodiff()) {
#ifndef LUISA_ENABLE_IR
        LUISA_ERROR_WITH_LOCATION(
            "Autodiff requires IR support but "
            "LuisaCompute is built without the IR module. "
            "This might be caused by missing Rust. "
            "Please install the Rust toolchain and "
            "recompile LuisaCompute to get the IR module.");
#else
        LUISA_VERBOSE_WITH_LOCATION("Performing AutoDiff transform "
                                    "on function with hash {:016x}.",
                                    function.hash());

        luisa::shared_ptr<const FunctionBuilder> converted;
        if (function.tag() == Function::Tag::KERNEL) {
            auto m = AST2IR::build_kernel(function);
            perform_autodiff_transform(m->get());
            converted = IR2AST::build(m->get());
        } else {
            auto m = AST2IR::build_callable(function);
            perform_autodiff_transform(m->get());
            converted = IR2AST::build(m->get());
        }
        LUISA_VERBOSE_WITH_LOCATION("Converted IR to AST for "
                                    "kernel with hash {:016x}. "
                                    "AutoDiff transform is done.",
                                    function.hash());
        return converted;
#endif
    }
    return function.shared_builder();
}

luisa::shared_ptr<const FunctionBuilder> transform_coroutine(
    Type *corotype,
    luisa::unordered_map<uint, luisa::shared_ptr<const FunctionBuilder>> &sub_builders,
    Function function) noexcept {
    if (true) {
#ifndef LUISA_ENABLE_IR
        LUISA_ERROR_WITH_LOCATION(
            "Coroutine requires IR support but "
            "LuisaCompute is built without the IR module. "
            "This might be caused by missing Rust. "
            "Please install the Rust toolchain and "
            "recompile LuisaCompute to get the IR module.");
#else
        LUISA_VERBOSE_WITH_LOCATION("Performing Coroutine transform "
                                    "on function with hash {:016x}.",
                                    function.hash());

        auto make_wrapper = [&](const FunctionBuilder *sub) noexcept {
            return FunctionBuilder::define_callable([&] {
                luisa::vector<const Expression *> args;
                args.reserve(function.arguments().size());
                LUISA_ASSERT(function.arguments().size() == function.bound_arguments().size(),
                             "Invalid capture list size (expected {}, got {}).",
                             function.arguments().size(), function.bound_arguments().size());
                auto fb = FunctionBuilder::current();
                for (auto arg_i = 0u; arg_i < function.arguments().size(); arg_i++) {
                    auto def_arg = function.arguments()[arg_i];
                    auto internal_arg = luisa::visit(
                        [&](auto b) noexcept -> const Expression * {
                            using T = std::decay_t<decltype(b)>;
                            if constexpr (std::is_same_v<T, Function::BufferBinding>) {
                                return fb->buffer_binding(def_arg.type(), b.handle, b.offset, b.size);
                            } else if constexpr (std::is_same_v<T, Function::TextureBinding>) {
                                return fb->texture_binding(def_arg.type(), b.handle, b.level);
                            } else if constexpr (std::is_same_v<T, Function::BindlessArrayBinding>) {
                                return fb->bindless_array_binding(b.handle);
                            } else if constexpr (std::is_same_v<T, Function::AccelBinding>) {
                                return fb->accel_binding(b.handle);
                            } else {
                                static_assert(std::is_same_v<T, luisa::monostate>);
                                switch (def_arg.tag()) {
                                    case Variable::Tag::REFERENCE: return fb->reference(def_arg.type());
                                    case Variable::Tag::BUFFER: return fb->buffer(def_arg.type());
                                    case Variable::Tag::TEXTURE: return fb->texture(def_arg.type());
                                    case Variable::Tag::BINDLESS_ARRAY: return fb->bindless_array();
                                    case Variable::Tag::ACCEL: return fb->accel();
                                    default: /* value argument */ return fb->argument(def_arg.type());
                                }
                            }
                        },
                        function.bound_arguments()[arg_i]);
                    args.emplace_back(internal_arg);
                }
                LUISA_ASSERT(sub->return_type() == nullptr,
                             "Coroutine subroutines should not have return type.");
                fb->call(sub->function(), args);
            });
        };

        //idea: send in function-> module with .subroutine-> seperate transform to callable-> register to coroutine
        luisa::shared_ptr<const FunctionBuilder> converted;
        auto m = AST2IR::build_coroutine(function);
        perform_coroutine_transform(m->get());
        converted = IR2AST::build(m->get());
        auto subroutines = m->get()->subroutines;
        auto subroutine_ids = m->get()->subroutine_ids;
        auto coroframe = corotype;
        auto coroframe_new = converted->arguments()[0].type();
        coroframe->update_from(coroframe_new);
        const_cast<FunctionBuilder *>(converted.get())->coroframe_replace(corotype);
        for (int i = 0; i < subroutines.len; ++i) {
            auto sub = IR2AST::build(subroutines.ptr[i]._0.get());
            const_cast<FunctionBuilder *>(sub.get())->coroframe_replace(corotype);
            auto wrapper = make_wrapper(sub.get());
            sub_builders.insert(std::make_pair(subroutine_ids.ptr[i], wrapper));
        }
        LUISA_VERBOSE_WITH_LOCATION("Converted IR to AST for "
                                    "kernel with hash {:016x}. "
                                    "Coroutine transform is done.",
                                    function.hash());
        return make_wrapper(converted.get());
#endif
    }
    return function.shared_builder();
}
}// namespace luisa::compute::detail
