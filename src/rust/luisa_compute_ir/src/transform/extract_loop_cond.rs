use std::collections::HashMap;
use super::Transform;

use crate::ir::{BasicBlock, Instruction, INVALID_REF, IrBuilder, luisa_compute_ir_build_local_zero_init, Module, NodeRef, Primitive, Type};
use crate::{CArc, Pooled};

struct ExtractLoopCondImpl {
    module: Module,
    loop2old: HashMap<NodeRef, NodeRef>,
    old2new: HashMap<NodeRef, NodeRef>,
}

impl ExtractLoopCondImpl {
    fn extract_loop_cond(module: Module) -> Module {
        let mut impl_ = Self {
            module,
            loop2old: HashMap::new(),
            old2new: HashMap::new(),
        };
        let bb = impl_.module.entry.clone();
        impl_.transform_bb(&bb);
        impl_.module
    }

    fn transform_bb(&mut self, basic_block: &Pooled<BasicBlock>) {
        let pools = self.module.pools.clone();
        let update_new_cond = |old: NodeRef, new: NodeRef, insert_point: NodeRef| {
            let mut builder = IrBuilder::new(pools.clone());
            builder.set_insert_point(insert_point);
            builder.update(new, old);
        };

        let mut node = basic_block.first.get().next;
        while node != basic_block.last {
            let instruction = node.get().instruction.as_ref();
            match instruction {
                Instruction::Loop { cond, body } => {
                    // insert new node (local) for cond
                    let mut builder = IrBuilder::new(self.module.pools.clone());
                    builder.set_insert_point(node.get().prev);
                    let type_ = CArc::new(Type::Primitive(Primitive::Bool));
                    let cond_new = builder.local_zero_init(type_);
                    self.old2new.insert(*cond, cond_new);

                    // record
                    self.loop2old.insert(node, *cond);
                    // process body
                    self.transform_bb(body);

                    // change loop cond to new node
                    node.get_mut().instruction = CArc::new(Instruction::Loop { cond: cond_new, body: *body });
                }
                Instruction::If { true_branch, false_branch, cond } => {
                    self.transform_bb(true_branch);
                    self.transform_bb(false_branch);
                }
                Instruction::Switch { value, cases, default } => {
                    for case in cases.as_ref() {
                        let block = case.block;
                        self.transform_bb(&block);
                    }
                    self.transform_bb(default);
                }

                // possible value instructions sources of cond
                Instruction::Local { .. } => {
                    if let Some(new) = self.old2new.get(&node) {
                        update_new_cond(node, *new, node);
                    }
                }
                Instruction::Const(..) => {
                    if let Some(new) = self.old2new.get(&node) {
                        update_new_cond(node, *new, node);
                    }
                }
                Instruction::Update { var, value } => {
                    if let Some(new) = self.old2new.get(var) {
                        update_new_cond(*var, *new, node);
                    }
                }
                Instruction::Call(..) => {
                    if let Some(new) = self.old2new.get(&node) {
                        update_new_cond(node, *new, node);
                    }
                }
                Instruction::Phi(..) => {
                    if let Some(new) = self.old2new.get(&node) {
                        update_new_cond(node, *new, node);
                    }
                }

                _ => {}
            }

            node = node.get().next;
        }
    }
}


pub struct ExtractLoopCond;


impl Transform for ExtractLoopCond {
    fn transform_module(&self, module: Module) -> Module {
        ExtractLoopCondImpl::extract_loop_cond(module)
    }
}