/*
 * This file implements the control flow canonicalization transform.
 * This transform removes all break/continue/early-return statements, in the following steps:
 * 1. Lower generic loops to do-while loops.
 * 2.
 */

use crate::ir::debug::dump_ir_human_readable;
use crate::ir::{
    new_node, BasicBlock, Const, Func, Instruction, IrBuilder, Module, ModulePools, Node, NodeRef,
};
use crate::transform::Transform;
use crate::{CArc, CBoxedSlice, Pooled, TypeOf};
use log::debug;
use std::collections::{HashMap, HashSet};

pub struct CanonicalizeControlFlow;

/*
 * This transform lowers generic loops to do-while loops as the following template shows:
 * - Original
 *   generic_loop {
 *       prepare;// typically the computation of the loop condition
 *       if cond {
 *           body;
 *           update; // continue goes here
 *       }
 *   }
 * - Transformed
 *   do {
 *       loop_break = false;
 *       prepare();
 *       if (!cond()) break;
 *       loop {
 *           body {
 *               // break => { loop_break = true; break; }
 *               // continue => { break; }
 *           }
 *           break;
 *       }
 *       if (loop_break) break;
 *       update();
 *   } while (true)
 */
struct LowerGenericLoops {
    pools: CArc<ModulePools>,
    transformed: HashSet<*const BasicBlock>,
    generic_loop_break: Option<NodeRef>,
}

impl LowerGenericLoops {
    fn transform_generic_loop(&mut self, node: NodeRef) {
        let mut builder = IrBuilder::new(self.pools.clone());
        builder.set_insert_point(node.get().prev);
        builder.comment(CBoxedSlice::from("lowered generic loop".to_string()));
        let const_true = builder.const_(Const::Bool(true));
        let const_false = builder.const_(Const::Bool(false));
        let loop_body = match node.get().instruction.as_ref() {
            Instruction::GenericLoop {
                prepare,
                cond,
                body,
                update,
            } => {
                // create a new block for the transformed generic loop
                let mut builder = IrBuilder::new(self.pools.clone());
                builder.comment(CBoxedSlice::from(
                    "lowered generic loop: break_flag = false".to_string(),
                ));
                let loop_break = builder.local(const_false);
                self.generic_loop_break = Some(loop_break.clone());
                // recursively transform the nested blocks
                self.transform_block(prepare);
                self.transform_block(body);
                self.transform_block(update);
                // move the prepare block to the new block
                builder.comment(CBoxedSlice::from(
                    "lowered generic loop: { prepare }".to_string(),
                ));
                builder.append_block(prepare.clone());
                // insert the loop condition
                builder.comment(CBoxedSlice::from(
                    "lowered generic loop: if (!cond) { break; }".to_string(),
                ));
                let cond = builder.call(Func::Not, &[cond.clone()], cond.type_().clone());
                let (if_cond_true, if_cond_false) = {
                    let mut true_branch_builder = IrBuilder::new(self.pools.clone());
                    true_branch_builder.break_();
                    let mut false_branch_builder = IrBuilder::new(self.pools.clone());
                    (true_branch_builder.finish(), false_branch_builder.finish())
                };
                builder.if_(cond.clone(), if_cond_true, if_cond_false);
                // build the loop body
                builder.comment(CBoxedSlice::from(
                    "lowered generic loop: loop { body; break; }".to_string(),
                ));
                let loop_body = {
                    let mut loop_body_builder = IrBuilder::new(self.pools.clone());
                    loop_body_builder.append_block(body.clone());
                    loop_body_builder.break_();
                    loop_body_builder.finish()
                };
                builder.loop_(loop_body, const_true);
                // insert the loop break
                builder.comment(CBoxedSlice::from(
                    "lowered generic loop: if (break_flag) { break; }".to_string(),
                ));
                let (if_loop_break_true, if_loop_break_false) = {
                    let mut true_branch_builder = IrBuilder::new(self.pools.clone());
                    true_branch_builder.break_();
                    let mut false_branch_builder = IrBuilder::new(self.pools.clone());
                    (true_branch_builder.finish(), false_branch_builder.finish())
                };
                let loop_break = builder.load(loop_break);
                builder.if_(loop_break, if_loop_break_true, if_loop_break_false);
                // insert the loop update
                builder.comment(CBoxedSlice::from(
                    "lowered generic loop: { update }".to_string(),
                ));
                builder.append_block(update.clone());
                builder.finish()
            }
            _ => unreachable!(),
        };
        node.get_mut().instruction = CArc::new(Instruction::Loop {
            body: loop_body,
            cond: const_true,
        });
    }

    fn transform_block(&mut self, bb: &Pooled<BasicBlock>) {
        for node in bb.iter() {
            match node.get().instruction.as_ref() {
                Instruction::Buffer => {}
                Instruction::Bindless => {}
                Instruction::Texture2D => {}
                Instruction::Texture3D => {}
                Instruction::Accel => {}
                Instruction::Shared => {}
                Instruction::Uniform => {}
                Instruction::Local { .. } => {}
                Instruction::Argument { .. } => {}
                Instruction::UserData(_) => {}
                Instruction::Invalid => {}
                Instruction::Const(_) => {}
                Instruction::Update { .. } => {}
                Instruction::Call(func, _) => match func {
                    Func::Callable(callable) => {
                        self.transform_module(&callable.0.module);
                    }
                    _ => {}
                },
                Instruction::Phi(_) => {
                    panic!("Phi nodes should be eliminated before this transform");
                }
                Instruction::Return(_) => {}
                Instruction::Loop { body, cond: _ } => {
                    let old_loop_break = self.generic_loop_break.take();
                    self.transform_block(body);
                    self.generic_loop_break = old_loop_break;
                }
                Instruction::GenericLoop { .. } => {
                    let old_loop_break = self.generic_loop_break.take();
                    self.transform_generic_loop(node);
                    self.generic_loop_break = old_loop_break;
                }
                Instruction::Break => {
                    if let Some(generic_loop_break) = self.generic_loop_break {
                        // break => { loop_break = true; break; }
                        let mut builder = IrBuilder::new(self.pools.clone());
                        builder.set_insert_point(node.get().prev);
                        builder.comment(CBoxedSlice::from(
                            "lowered generic loop: break => { break_flag = true; break; }"
                                .to_string(),
                        ));
                        let const_true = builder.const_(Const::Bool(true));
                        builder.update(generic_loop_break.clone(), const_true);
                    }
                }
                Instruction::Continue => {
                    if let Some(generic_loop_break) = self.generic_loop_break {
                        // continue => { break; }
                        let mut builder = IrBuilder::new(self.pools.clone());
                        builder.set_insert_point(node.get().prev);
                        builder.comment(CBoxedSlice::from(
                            "lowered generic loop: continue => { break; }".to_string(),
                        ));
                        node.get_mut().instruction = CArc::new(Instruction::Break);
                    }
                }
                Instruction::If {
                    cond: _,
                    true_branch,
                    false_branch,
                } => {
                    self.transform_block(true_branch);
                    self.transform_block(false_branch);
                }
                Instruction::Switch {
                    value: _,
                    default,
                    cases,
                } => {
                    self.transform_block(default);
                    for case in cases.iter() {
                        self.transform_block(&case.block);
                    }
                }
                Instruction::AdScope {
                    body,
                    forward: _,
                    n_forward_grads: _,
                } => {
                    self.transform_block(body);
                }
                Instruction::RayQuery {
                    ray_query: _,
                    on_triangle_hit,
                    on_procedural_hit,
                } => {
                    self.transform_block(on_triangle_hit);
                    self.transform_block(on_procedural_hit);
                }
                Instruction::Print { .. } => {}
                Instruction::AdDetach(body) => {
                    self.transform_block(body);
                }
                Instruction::Comment(_) => {}
                Instruction::CoroSplitMark { .. } => {}
                Instruction::CoroSuspend { .. } => {}
                Instruction::CoroResume { .. } => {}
                Instruction::CoroRegister { .. } => {}
            }
        }
    }

    fn transform_module(&mut self, module: &Module) {
        if self.transformed.insert(module.entry.as_ptr()) {
            // not transformed before, so transform it
            self.transform_block(&module.entry)
        }
    }

    pub fn transform(module: Module) -> Module {
        let mut lower_generic_loops = LowerGenericLoops {
            pools: module.pools.clone(),
            transformed: HashSet::new(),
            generic_loop_break: None,
        };
        lower_generic_loops.transform_module(&module);
        module
    }
}

impl Transform for CanonicalizeControlFlow {
    fn transform_module(&self, module: Module) -> Module {
        // 1. Lower generic loops to do-while loops.
        println!(
            "Before LowerGenericLoops::transform:\n{}",
            dump_ir_human_readable(&module)
        );
        let module = LowerGenericLoops::transform(module);
        println!(
            "After LowerGenericLoops::transform:\n{}",
            dump_ir_human_readable(&module)
        );
        module // TODO
    }
}
