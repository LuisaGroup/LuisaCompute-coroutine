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
    // ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "canonicalize_control_flow");
    // ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "demote_locals");
    // ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "defer_load");
    // ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "extract_loop_cond");
    // ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "split_coro");
    ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "materialize_coro");
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

}// namespace luisa::compute::detail
