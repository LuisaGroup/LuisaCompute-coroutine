use crate::ir::{BasicBlock, CallableModule, Module, ModulePools, NodeRef};
use crate::transform::Transform;
use crate::{CArc, Pooled};
use std::collections::HashMap;

struct CopyPropagationImpl {
    pools: CArc<ModulePools>,
    module_original: Pooled<BasicBlock>,

    old2new: HashMap<NodeRef, NodeRef>,
}

impl CopyPropagationImpl {
    fn new(module: &Module) -> Self {
        Self {
            pools: module.pools.clone(),
            module_original: module.entry,
            old2new: HashMap::new(),
        }
    }
}

pub struct CopyPropagation;

impl Transform for CopyPropagation {
    fn transform_callable(&self, module: CallableModule) -> CallableModule {
        let mut impl_ = CopyPropagationImpl::new(&module.module);

        // TODO: copy propagation

        // TODO: especially phi of callable args!

        todo!()
    }
}
