#include <luisa/core/logging.h>
#include <luisa/dsl/func.h>

#ifdef LUISA_ENABLE_IR
#include <luisa/ir/ir2ast.h>
#include <luisa/ir/ast2ir.h>
#endif

namespace luisa::compute::detail {

void CallableInvoke::_error_too_many_arguments() noexcept {
    LUISA_ERROR_WITH_LOCATION("Too many arguments for callable.");
}

}// namespace luisa::compute::detail

namespace luisa::compute::detail {

#ifdef LUISA_ENABLE_IR
template<typename M>
void perform_autodiff_transform(M *m) noexcept {
    auto autodiff_pipeline = ir::luisa_compute_ir_transform_pipeline_new();
    ir::luisa_compute_ir_transform_pipeline_add_transform(autodiff_pipeline, "autodiff");
    auto converted_module = ir::luisa_compute_ir_transform_pipeline_transform_module(autodiff_pipeline, m->module);
    ir::luisa_compute_ir_transform_pipeline_destroy(autodiff_pipeline);
    m->module = converted_module;
}

template<typename M>
void perform_coroutine_transform(M *m) noexcept {
    auto coroutine_pipeline = ir::luisa_compute_ir_transform_pipeline_new();
    ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "coroutine");
    auto converted_module = ir::luisa_compute_ir_transform_pipeline_transform_module(coroutine_pipeline, m->module);
    ir::luisa_compute_ir_transform_pipeline_destroy(coroutine_pipeline);
    m->module = converted_module;
}
#endif

luisa::shared_ptr<const FunctionBuilder>
transform_function(Function function) noexcept {

#ifndef LUISA_IR_SUPPORT_ERROR_WITH_LOCATION
#define LUISA_IR_SUPPORT_ERROR_WITH_LOCATION(module_name)           \
    LUISA_ERROR_WITH_LOCATION(                                      \
        module_name " requires IR support but "                     \
                    "LuisaCompute is built without the IR module. " \
                    "This might be caused by missing Rust. "        \
                    "Please install the Rust toolchain and "        \
                    "recompile LuisaCompute to get the IR module.")

    // Note: we only consider the direct builtin callables here since the indirect
    //       ones should have been transformed by the called function.

    if (!function.direct_builtin_callables().uses_autodiff() && !function.direct_builtin_callables().uses_coroutine()) {
        return function.shared_builder();
    }

    luisa::shared_ptr<const FunctionBuilder> converted;
    if (function.tag() == Function::Tag::KERNEL) {
        auto m = AST2IR::build_kernel(function);

        if (function.direct_builtin_callables().uses_autodiff()) {
#ifndef LUISA_ENABLE_IR
            LUISA_IR_SUPPORT_ERROR_WITH_LOCATION("AutoDiff");
#else
            LUISA_VERBOSE_WITH_LOCATION("Performing AutoDiff transform "
                                        "on function with hash {:016x}.",
                                        function.hash());
#endif
            perform_autodiff_transform(m->get());
        }

        if (function.direct_builtin_callables().uses_coroutine()) {
#ifndef LUISA_ENABLE_IR
            LUISA_IR_SUPPORT_ERROR_WITH_LOCATION("Coroutine");
#else
            LUISA_VERBOSE_WITH_LOCATION("Performing Coroutine transform "
                                        "on function with hash {:016x}.",
                                        function.hash());
            perform_coroutine_transform(m->get());
#endif
        }

        converted = IR2AST::build(m->get());
    } else {
        auto m = AST2IR::build_callable(function);

        if (function.direct_builtin_callables().uses_autodiff()) {
#ifndef LUISA_ENABLE_IR
            LUISA_IR_SUPPORT_ERROR_WITH_LOCATION("AutoDiff");
#else
            LUISA_VERBOSE_WITH_LOCATION("Performing AutoDiff transform "
                                        "on function with hash {:016x}.",
                                        function.hash());
#endif
            perform_autodiff_transform(m->get());
        }

        if (function.direct_builtin_callables().uses_coroutine()) {
#ifndef LUISA_ENABLE_IR
            LUISA_IR_SUPPORT_ERROR_WITH_LOCATION("Coroutine");
#else
            LUISA_VERBOSE_WITH_LOCATION("Performing Coroutine transform "
                                        "on function with hash {:016x}.",
                                        function.hash());
            perform_coroutine_transform(m->get());
#endif
        }

        converted = IR2AST::build(m->get());
    }

    LUISA_VERBOSE_WITH_LOCATION("Converted IR to AST for "
                                "kernel with hash {:016x}. "
                                "Transform is done.",
                                function.hash());
    return converted;

#undef LUISA_IR_SUPPORT_ERROR_WITH_LOCATION
#endif
}

}// namespace luisa::compute::detail
