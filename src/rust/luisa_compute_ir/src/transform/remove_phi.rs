use std::collections::HashSet;
use crate::ir::{BasicBlock, collect_nodes, Func, Instruction, IrBuilder, Module};
use crate::transform::Transform;

struct RemovePhiImpl;

impl RemovePhiImpl {
    fn remove_phis_recursive(module: &Module, visited: &mut HashSet<*const BasicBlock>) {
        if visited.insert(module.entry.as_ptr()) {
            let nodes = collect_nodes(module.entry.clone());
            for node in nodes {
                match node.get().instruction.as_ref() {
                    Instruction::Phi(incomings) => {
                        let mut b = IrBuilder::new_without_bb(module.pools.clone());
                        b.set_insert_point(module.entry.first);
                        let local = b.local_zero_init(node.type_().clone());
                        for incoming in incomings.iter() {
                            b.set_insert_point(incoming.block.last.get().prev);
                            b.update(local, incoming.value);
                        }
                        b.set_insert_point(node);
                        let load = b.load(local);
                        load.remove();
                        node.replace_with(load.get());
                    }
                    Instruction::Call(func, args) => {
                        if let Func::Callable(callable) = func {
                            Self::remove_phis_recursive(&callable.0.module, visited);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    fn remove_phis(module: &Module) {
        let mut visited = HashSet::new();
        Self::remove_phis_recursive(module, &mut visited);
    }
}

pub struct RemovePhi;

impl Transform for RemovePhi {
    fn transform_module(&self, module: Module) -> Module {
        RemovePhiImpl::remove_phis(&module);
        module
    }
}