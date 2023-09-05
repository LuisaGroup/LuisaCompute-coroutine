pub mod autodiff;
pub mod coroutine;
pub mod lower_control_flow;
pub mod ssa;
// pub mod validate;
pub mod vectorize;
pub mod eval;
use crate::ir::{self, CallableModule, Module, KernelModule};

pub trait Transform {
    fn transform_module(&self, module: Module)-> Module{
        panic!("transform module not implemented")
    }
    fn transform_callable(&self, module: CallableModule)->CallableModule{
        CallableModule{
            module: self.transform_module(module.module),
            ..module
        }
    }
    fn transform_kernel(&self, kernel: KernelModule)->KernelModule{
        KernelModule{
            module: self.transform_module(kernel.module),
            ..kernel
        }
    }
}

pub struct TransformPipeline {
    transforms: Vec<Box<dyn Transform>>,
}
impl TransformPipeline {
    pub fn new() -> Self {
        Self {
            transforms: Vec::new(),
        }
    }
    pub fn add_transform(&mut self, transform: Box<dyn Transform>) {
        self.transforms.push(transform);
    }
}
impl Transform for TransformPipeline {
    fn transform_module(&self, module: Module) -> Module {
        let mut module = module;
        for transform in &self.transforms {
            module = transform.transform_module(module);
        }
        module
    }
    fn transform_callable(&self, module: CallableModule) -> CallableModule {
        let mut module = module;
        for transform in &self.transforms {
            module = transform.transform_callable(module);
        }
        module
    }
    fn transform_kernel(&self, module: KernelModule) -> KernelModule {
        let mut module = module;
        for transform in &self.transforms {
            module = transform.transform_kernel(module);
        }
        module
    }
}

#[no_mangle]
pub extern "C" fn luisa_compute_ir_transform_pipeline_new() -> *mut TransformPipeline {
    Box::into_raw(Box::new(TransformPipeline::new()))
}

#[no_mangle]
pub extern "C" fn luisa_compute_ir_transform_pipeline_add_transform(
    pipeline: *mut TransformPipeline,
    name: *const std::os::raw::c_char,
) {
    let name = unsafe { std::ffi::CStr::from_ptr(name) }
        .to_str()
        .unwrap()
        .to_string();
    match name.as_str() {
        "ssa" => {
            let transform = ssa::ToSSA;
            unsafe { (*pipeline).add_transform(Box::new(transform)) };
        }
        // "lower_control_flow"=>{
        //     let transform = lower_control_flow::LowerControlFlow::new();
        //     unsafe { (*pipeline).add_transform(Box::new(transform)) };
        // }
        // "vectorize"=>{
        //     let transform = vectorize::Vectorize::new();
        //     unsafe { (*pipeline).add_transform(Box::new(transform)) };
        // }
        "autodiff" => {
            let transform = autodiff::Autodiff;
            unsafe { (*pipeline).add_transform(Box::new(transform)) };
        }
        "coroutine"=>{
            let transform = coroutine::Coroutine;
            unsafe { (*pipeline).add_transform(Box::new(transform)) };
        }
        _ => panic!("unknown transform {}", name),
    }
}

#[no_mangle]
pub extern "C" fn luisa_compute_ir_transform_pipeline_transform_module(
    pipeline: *mut TransformPipeline,
    module: Module,
) -> Module {
    unsafe { (*pipeline).transform_module(module) }
}
#[no_mangle]
pub extern "C" fn luisa_compute_ir_transform_pipeline_transform_callable(
    pipeline: *mut TransformPipeline,
    module: CallableModule,
) -> CallableModule {
    unsafe { (*pipeline).transform_callable(module) }
}
#[no_mangle]
pub extern "C" fn luisa_compute_ir_transform_pipeline_transform_kernel(
    pipeline: *mut TransformPipeline,
    module: KernelModule,
) -> KernelModule {
    unsafe { (*pipeline).transform_kernel(module) }
}

#[no_mangle]
pub extern "C" fn luisa_compute_ir_transform_pipeline_destroy(pipeline: *mut TransformPipeline) {
    unsafe {
        std::mem::drop(Box::from_raw(pipeline));
    }
}
