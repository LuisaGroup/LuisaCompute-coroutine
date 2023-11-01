/*
 * This file implements the control flow canonicalization transform.
 * This transform removes all break/continue/early-return statements, in the following steps:
 * 1. Lower generic loops to do-while loops.
 * 2.
 */

use crate::ir::debug::dump_ir_human_readable;
use crate::ir::{BasicBlock, Const, Func, Instruction, IrBuilder, Module, ModulePools, NodeRef};
use crate::transform::Transform;
use crate::{CArc, CBoxedSlice, Pooled, TypeOf};
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
 *       do {
 *           body {
 *               // break => { loop_break = true; break; }
 *               // continue => { break; }
 *           }
 *       } while (false)
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
                    "lowered generic loop: do { body } while (false)".to_string(),
                ));
                builder.loop_(body.clone(), const_false);
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
            let old_loop_break = self.generic_loop_break.take();
            let old_pools = self.pools.clone();
            self.pools = module.pools.clone();
            self.transform_block(&module.entry);
            self.generic_loop_break = old_loop_break;
            self.pools = old_pools;
        }
    }

    fn transform(module: &Module) {
        let mut lower_generic_loops = LowerGenericLoops {
            pools: CArc::null(),
            transformed: HashSet::new(),
            generic_loop_break: None,
        };
        lower_generic_loops.transform_module(module);
    }
}

// This analysis checks whether a flag variable is necessary in a loop.
// If so, it generates the flag variable and records it with the loop.
struct BreakContinueFlags {
    break_: HashMap<NodeRef, NodeRef>,
    continue_: HashMap<NodeRef, NodeRef>,
}

struct LowerBreakContinuePreprocess {
    pools: CArc<ModulePools>,
    processed: HashSet<*const BasicBlock>,
    flags: BreakContinueFlags,
    current_loop: Option<NodeRef>,
}

impl LowerBreakContinuePreprocess {
    fn process_block(&mut self, bb: &Pooled<BasicBlock>) {
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
                        self.process_module(&callable.0.module);
                    }
                    _ => {}
                },
                Instruction::Phi(_) => {
                    panic!("Phi nodes should be eliminated before this transform");
                }
                Instruction::Return(_) => {
                    // remove all nodes after the return node as they are unreachable
                    while !node.get().next.is_sentinel() {
                        node.get().next.remove();
                    }
                }
                Instruction::Loop { body, cond: _ } => {
                    let old_current_loop = self.current_loop.take();
                    self.current_loop = Some(node);
                    self.process_block(body);
                    self.current_loop = old_current_loop;
                }
                Instruction::GenericLoop { .. } => {
                    panic!("GenericLoop nodes should be eliminated before this transform");
                }
                Instruction::Break => {
                    if let Some(current_loop) = self.current_loop {
                        // if the flag variable is not generated yet, generate it
                        if !self.flags.break_.contains_key(&current_loop) {
                            let mut builder = IrBuilder::new(self.pools.clone());
                            builder.set_insert_point(current_loop.get().prev);
                            builder.comment(CBoxedSlice::from("loop break flag".to_string()));
                            let const_false = builder.const_(Const::Bool(false));
                            let loop_break = builder.local(const_false);
                            self.flags.break_.insert(current_loop.clone(), loop_break);
                        }
                    } else {
                        // error
                        panic!("Break outside of loop");
                    }
                    // remove all nodes after the break node as they are unreachable
                    while !node.get().next.is_sentinel() {
                        node.get().next.remove();
                    }
                }
                Instruction::Continue => {
                    if let Some(current_loop) = self.current_loop {
                        if !self.flags.continue_.contains_key(&current_loop) {
                            let mut builder = IrBuilder::new(self.pools.clone());
                            builder.set_insert_point(current_loop.get().prev);
                            builder.comment(CBoxedSlice::from("loop continue flag".to_string()));
                            let const_false = builder.const_(Const::Bool(false));
                            let loop_continue = builder.local(const_false);
                            self.flags
                                .continue_
                                .insert(current_loop.clone(), loop_continue);
                        }
                    } else {
                        // error
                        panic!("Continue outside of loop");
                    }
                    // remove all nodes after the continue node as they are unreachable
                    while !node.get().next.is_sentinel() {
                        node.get().next.remove();
                    }
                }
                Instruction::If {
                    cond: _,
                    true_branch,
                    false_branch,
                } => {
                    self.process_block(true_branch);
                    self.process_block(false_branch);
                }
                Instruction::Switch {
                    value: _,
                    default,
                    cases,
                } => {
                    self.process_block(default);
                    for case in cases.iter() {
                        self.process_block(&case.block);
                    }
                }
                Instruction::AdScope { body, .. } => {
                    self.process_block(body);
                }
                Instruction::RayQuery {
                    on_triangle_hit,
                    on_procedural_hit,
                    ..
                } => {
                    self.process_block(on_triangle_hit);
                    self.process_block(on_procedural_hit);
                }
                Instruction::Print { .. } => {}
                Instruction::AdDetach(body) => {
                    self.process_block(body);
                }
                Instruction::Comment(_) => {}
                Instruction::CoroSplitMark { .. } => {}
                Instruction::CoroSuspend { .. } => {}
                Instruction::CoroResume { .. } => {}
                Instruction::CoroRegister { .. } => {}
            }
        }
    }

    fn process_module(&mut self, module: &Module) {
        if self.processed.insert(module.entry.as_ptr()) {
            // not processed before, so process it
            let old_current_loop = self.current_loop.take();
            let old_pools = self.pools.clone();
            self.pools = module.pools.clone();
            self.process_block(&module.entry);
            self.current_loop = old_current_loop;
            self.pools = old_pools;
        }
    }

    fn process(module: &Module) -> BreakContinueFlags {
        let mut lower_break_continue_preprocess = LowerBreakContinuePreprocess {
            pools: CArc::null(),
            processed: HashSet::new(),
            flags: BreakContinueFlags {
                break_: HashMap::new(),
                continue_: HashMap::new(),
            },
            current_loop: None,
        };
        lower_break_continue_preprocess.process_module(&module);
        lower_break_continue_preprocess.flags
    }
}

struct LowerBreakContinue {
    pools: CArc<ModulePools>,
    current_loop: Option<NodeRef>,
    flags: BreakContinueFlags,
    transformed: HashSet<*const BasicBlock>,
    loops: Vec<(CArc<ModulePools>, NodeRef)>,
}

impl LowerBreakContinue {
    fn extract_guarded_block(&self, node: NodeRef) -> Pooled<BasicBlock> {
        let mut builder = IrBuilder::new(self.pools.clone());
        while !node.get().next.is_sentinel() {
            let next = node.get().next;
            next.remove();
            builder.append(next);
        }
        builder.finish()
    }

    fn empty_block(&self) -> Pooled<BasicBlock> {
        let mut builder = IrBuilder::new(self.pools.clone());
        builder.finish()
    }

    fn transform_control_flow(&mut self, node: NodeRef, flag: NodeRef, stack: &Vec<NodeRef>) {
        assert_eq!(stack.last(), Some(&node));
        let mut builder = IrBuilder::new(self.pools.clone());
        builder.set_insert_point(node.get().prev);
        // mark the loop break/continue flag as true
        builder.comment(CBoxedSlice::from(
            "lower control flow: flag = true".to_string(),
        ));
        let const_true = builder.const_(Const::Bool(true));
        let update = builder.update(flag.clone(), const_true);
        // remove the break/continue node and replace it with the update
        update.remove();
        node.replace_with(update.get());
        // recursively split the loop into if-guarded blocks
        let current_loop = self.current_loop.as_ref().unwrap();
        for node in stack.iter().rev().skip(1) {
            if node == current_loop {
                break;
            }
            let guarded = self.extract_guarded_block(node.clone());
            if guarded.any_non_comment_node() {
                builder.set_insert_point(node.clone());
                builder.comment(CBoxedSlice::from(
                    "lower control flow: guarded block".to_string(),
                ));
                let flag = builder.load(flag.clone());
                let not_flag = builder.call(Func::Not, &[flag], flag.type_().clone());
                builder.if_(not_flag, guarded, self.empty_block());
            }
        }
    }

    fn transform_block(&mut self, bb: &Pooled<BasicBlock>, stack: &mut Vec<NodeRef>) {
        for node in bb.iter() {
            stack.push(node);
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
                Instruction::Loop { body, cond } => {
                    self.loops.push((self.pools.clone(), node.clone()));
                    let old_current_loop = self.current_loop.take();
                    self.current_loop = Some(node);
                    self.transform_block(body, stack);
                    self.current_loop = old_current_loop;
                    // reset the flag before the loop and edit the loop condition
                    let break_flag = self.flags.break_.get(&node);
                    let continue_flag = self.flags.continue_.get(&node);
                    if break_flag.is_some() || continue_flag.is_some() {
                        let mut builder = IrBuilder::new(self.pools.clone());
                        // reset the flag before the loop
                        builder.set_insert_point(body.first.clone());
                        if let Some(flag) = break_flag {
                            let const_false = builder.const_(Const::Bool(false));
                            builder.update(flag.clone(), const_false);
                        }
                        if let Some(flag) = continue_flag {
                            let const_false = builder.const_(Const::Bool(false));
                            builder.update(flag.clone(), const_false);
                        }
                        // edit the loop condition
                        builder.set_insert_point(body.last.get().prev);
                        // process the break flag: do { body } while (cond && !flag)
                        let cond = if let Some(flag) = break_flag {
                            let flag = builder.load(flag.clone());
                            let not_flag = builder.call(Func::Not, &[flag], flag.type_().clone());
                            if cond.is_const() {
                                // simple constant folding
                                if cond.get_bool() {
                                    not_flag // true && any => any
                                } else {
                                    cond.clone() // false && any => false
                                }
                            } else {
                                // not foldable
                                builder.call(
                                    Func::BitAnd,
                                    &[cond.clone(), not_flag],
                                    cond.type_().clone(),
                                )
                            }
                        } else {
                            cond.clone()
                        };
                        // process the continue flag: do { body } while (cond || flag)
                        let cond = if let Some(flag) = continue_flag {
                            let flag = builder.load(flag.clone());
                            if cond.is_const() {
                                // simple constant folding
                                if cond.get_bool() {
                                    cond.clone() // true || any => true
                                } else {
                                    flag // false || any => any
                                }
                            } else {
                                // not foldable
                                builder.call(
                                    Func::BitOr,
                                    &[cond.clone(), flag],
                                    cond.type_().clone(),
                                )
                            }
                        } else {
                            cond.clone()
                        };
                        // update the loop condition
                        node.get_mut().instruction = CArc::new(Instruction::Loop {
                            body: body.clone(),
                            cond,
                        });
                    }
                }
                Instruction::GenericLoop { .. } => {
                    panic!("GenericLoop nodes should be eliminated before this transform");
                }
                Instruction::Break => {
                    let flag = self
                        .flags
                        .break_
                        .get(&self.current_loop.as_ref().unwrap())
                        .unwrap();
                    self.transform_control_flow(node, flag.clone(), stack);
                }
                Instruction::Continue => {
                    let flag = self
                        .flags
                        .break_
                        .get(&self.current_loop.as_ref().unwrap())
                        .unwrap();
                    self.transform_control_flow(node, flag.clone(), stack);
                }
                Instruction::If {
                    true_branch,
                    false_branch,
                    ..
                } => {
                    self.transform_block(true_branch, stack);
                    self.transform_block(false_branch, stack);
                }
                Instruction::Switch { default, cases, .. } => {
                    self.transform_block(default, stack);
                    for case in cases.iter() {
                        self.transform_block(&case.block, stack);
                    }
                }
                Instruction::AdScope { body, .. } => {
                    self.transform_block(body, stack);
                }
                Instruction::RayQuery {
                    on_triangle_hit,
                    on_procedural_hit,
                    ..
                } => {
                    self.transform_block(on_triangle_hit, stack);
                    self.transform_block(on_procedural_hit, stack);
                }
                Instruction::Print { .. } => {}
                Instruction::AdDetach(body) => {
                    self.transform_block(body, stack);
                }
                Instruction::Comment(_) => {}
                Instruction::CoroSplitMark { .. } => {}
                Instruction::CoroSuspend { .. } => {}
                Instruction::CoroResume { .. } => {}
                Instruction::CoroRegister { .. } => {}
            }
            let popped = stack.pop();
            assert_eq!(popped, Some(node));
        }
    }

    fn transform_module(&mut self, module: &Module) {
        if self.transformed.insert(module.entry.as_ptr()) {
            let old_pools = self.pools.clone();
            let old_current_loop = self.current_loop.take();
            self.pools = module.pools.clone();
            let mut stack = Vec::new();
            self.transform_block(&module.entry, &mut stack);
            self.pools = old_pools;
            self.current_loop = old_current_loop;
        }
    }

    fn transform(module: &Module) {
        let mut lower_break_continue = LowerBreakContinue {
            pools: CArc::null(),
            current_loop: None,
            flags: LowerBreakContinuePreprocess::process(module),
            transformed: HashSet::new(),
            loops: Vec::new(),
        };
        lower_break_continue.transform_module(&module);
        // flatten do {} while (false) loops
        for (pool, loop_) in lower_break_continue.loops {
            if let Instruction::Loop { body, cond } = loop_.get().instruction.as_ref() {
                if cond.is_const() && !cond.get_bool() {
                    let mut builder = IrBuilder::new(pool);
                    builder.set_insert_point(loop_.get().prev);
                    loop_.remove();
                    builder.comment(CBoxedSlice::from("flattened loop begin".to_string()));
                    builder.append_block(body.clone());
                    builder.comment(CBoxedSlice::from("flattened loop end".to_string()));
                }
            }
        }
    }
}

// TODO
struct LowerEarlyReturn;

impl Transform for CanonicalizeControlFlow {
    fn transform_module(&self, module: Module) -> Module {
        // 1. Lower generic loops to do-while loops.
        println!(
            "Before LowerGenericLoops::transform:\n{}",
            dump_ir_human_readable(&module)
        );
        LowerGenericLoops::transform(&module);
        println!(
            "After LowerGenericLoops::transform:\n{}",
            dump_ir_human_readable(&module)
        );
        // 2. lower break/continue nodes
        LowerBreakContinuePreprocess::process(&module);
        println!(
            "After LowerBreakContinuePreprocess::process:\n{}",
            dump_ir_human_readable(&module)
        );
        LowerBreakContinue::transform(&module);
        println!(
            "After LowerBreakContinue::transform:\n{}",
            dump_ir_human_readable(&module)
        );
        module // TODO
    }
}
