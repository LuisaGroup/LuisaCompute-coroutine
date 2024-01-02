// This file implements the Mem2Reg pass to get an SSA form of the IR.
// We replace Local nodes' updates with Phi nodes.
// Reference paper: [2013] Simple and Efficient Construction of Static Single Assignment Form

use crate::analysis::utility::{is_primitives_read_only_function, DISPLAY_IR_DEBUG};
use crate::display::DisplayIR;
use crate::ir::{
    collect_nodes, new_node, ArrayType, BasicBlock, CallableModule, Func, Instruction, IrBuilder,
    MatrixType, Module, ModulePools, Node, NodeRef, PhiIncoming, Primitive, StructType, Type,
    VectorType,
};
use crate::transform::Transform;
use crate::{CArc, CBoxedSlice, Pooled};
use std::collections::{HashMap, HashSet};

#[derive(Default, Clone)]
struct IncomingInfo {
    defined_locally: HashSet<PhiIncoming>,
    alive: HashSet<PhiIncoming>,
    exclusive_defined_locally: bool,
    exclusive_alive: bool,
}

struct Mem2RegImpl {
    pools: CArc<ModulePools>,
    module_original: Pooled<BasicBlock>,

    node_definition: HashMap<NodeRef, HashMap<Pooled<BasicBlock>, IncomingInfo>>,
    node_updated: HashMap<Pooled<BasicBlock>, HashSet<NodeRef>>,
    local_remaining: HashSet<NodeRef>,
    phi2var: HashMap<NodeRef, NodeRef>,
    backfill_phis: HashMap<Pooled<BasicBlock>, HashSet<NodeRef>>,
}

impl Mem2RegImpl {
    fn new(module: &Module) -> Self {
        Self {
            pools: module.pools.clone(),
            module_original: module.entry.clone(),
            node_definition: HashMap::new(),
            node_updated: HashMap::new(),
            local_remaining: HashSet::new(),
            phi2var: HashMap::new(),
            backfill_phis: HashMap::new(),
        }
    }

    fn pre_process(&mut self) {
        self.scan_argument_by_ref(self.module_original);
    }
    fn scan_argument_by_ref(&mut self, block: Pooled<BasicBlock>) {
        let nodes = collect_nodes(block);
        for node in nodes.iter() {
            let type_ = node.type_().as_ref();
            let instruction = node.get().instruction.as_ref();
            match instruction {
                Instruction::Call(func, args) => {
                    if !is_primitives_read_only_function(func, args) {
                        match func {
                            Func::Callable(callable) => {
                                for (parameter, arg) in callable.0.args.iter().zip(args.iter()) {
                                    if arg.is_local_primitive() && parameter.is_reference_argument()
                                    {
                                        self.local_remaining.insert(*arg);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        for node in nodes.iter() {
            let type_ = node.type_().as_ref();
            let instruction = node.get().instruction.as_ref();
            match instruction {
                Instruction::Local { .. } => {
                    if !node.is_primitive() {
                        println!("local_remaining {:?}", *node);
                        self.local_remaining.insert(*node);
                    }
                }
                _ => {}
            }
        }
    }

    fn keep_unchanged(&self, var: NodeRef) -> bool {
        if var.is_local() {
            if self.local_remaining.contains(&var) {
                return true;
            }
            return false;
        }
        let is_load_removable_func = match var.get().instruction.as_ref() {
            Instruction::Call(func, args) => match func {
                Func::Load => {
                    assert_eq!(args.len(), 1);
                    let arg = args[0];
                    let ans = || {
                        if arg.is_gep() {
                            return false;
                        }
                        if arg.is_local() {
                            if self.local_remaining.contains(&arg) {
                                return false;
                            }
                            return true;
                        }
                        if arg.is_phi() {
                            return true;
                        }
                        false
                    };
                    ans()
                }
                _ => false,
            },
            _ => false,
        };
        if is_load_removable_func {
            return false;
        }
        true
    }
    fn record_def(&mut self, block: Pooled<BasicBlock>, var: NodeRef, value: NodeRef) {
        // if self.keep_unchanged(var) {
        //     return;
        // }

        // some PhiIncomings are killed by this def
        let incomings = self
            .node_definition
            .entry(var)
            .or_default()
            .entry(block)
            .or_default();
        incomings.alive.clear();
        incomings.defined_locally.clear();

        // record new def
        let incoming = PhiIncoming { value, block };
        incomings.defined_locally.insert(incoming);
        incomings.alive.insert(incoming);
        incomings.exclusive_defined_locally = true;
        incomings.exclusive_alive = true;
        self.node_updated.entry(block).or_default().insert(var);
    }
    /// Record use of a value.
    /// If a new phi node is created, return this phi node.
    /// Otherwise, return the original value.
    fn record_use(
        &mut self,
        block: Pooled<BasicBlock>,
        var: NodeRef,
        insert_before: NodeRef,
    ) -> NodeRef {
        if self.keep_unchanged(var) {
            return var;
        }

        // record use
        let incomings = self.node_definition.get(&var).unwrap().get(&block).unwrap();
        assert!(incomings.exclusive_alive); // there must be a value for each incoming block
        let incomings_alive = CBoxedSlice::new(incomings.alive.iter().cloned().collect::<Vec<_>>());

        // Optimize if there is only 1 incoming and do not need backfill in loops
        let mut backfill = false;
        if incomings.exclusive_defined_locally {
            // if incomings_alive.len() == 1 {
            //     return incomings_alive[0].value;
            // }
        } else {
            backfill = true;
        }

        let instruction = CArc::new(Instruction::Phi(incomings_alive));
        let phi = Node::new(instruction, var.type_().clone());
        let phi = new_node(&self.pools, phi);

        // We don't record_def(block, var, phi) here because it will override possible alive incomings.
        // Eliminate common subexpression in an other pass if possible.

        // record phis that need backfill in loops
        self.phi2var.insert(phi, var);
        if backfill {
            self.backfill_phis.entry(block).or_default().insert(phi);
        }

        // insert phi node before ref_node
        insert_before.insert_before_self(phi);
        phi
    }
    fn enter_block(&mut self, branch: Pooled<BasicBlock>, outer: Pooled<BasicBlock>) {
        for (node, incoming_info) in self.node_definition.iter_mut() {
            if let Some(incoming_outer) = incoming_info.get(&outer) {
                let incoming_branch = IncomingInfo {
                    defined_locally: HashSet::new(),
                    alive: incoming_outer.alive.clone(),
                    exclusive_defined_locally: false,
                    exclusive_alive: incoming_outer.exclusive_alive,
                };
                assert!(incoming_info.insert(branch, incoming_branch).is_none());
            }
        }
        assert!(self.node_updated.insert(branch, HashSet::new()).is_none());
    }
    fn exit_block(&mut self, branches: Vec<Pooled<BasicBlock>>, outer: Pooled<BasicBlock>) {
        let mut node_updated_branch = HashSet::new();
        let mut incomings_branch: HashMap<NodeRef, IncomingInfo> = HashMap::new();

        let branch_set: HashSet<Pooled<BasicBlock>> = HashSet::from_iter(branches.iter().cloned());
        assert_eq!(branches.len(), branch_set.len());

        for branch in branches.iter() {
            // add node updated by inner to outer
            let node_updated = self.node_updated.remove(branch).unwrap();
            node_updated_branch.extend(node_updated);
        }

        for branch in branches {
            // update incomings
            for node in node_updated_branch.iter() {
                let incoming_branch_merge = incomings_branch.entry(*node).or_insert(IncomingInfo {
                    defined_locally: HashSet::new(),
                    alive: HashSet::new(),
                    exclusive_defined_locally: true,
                    exclusive_alive: true, // value not used, does not matter
                });
                if let Some(incoming_branch) =
                    self.node_definition.get_mut(node).unwrap().remove(&branch)
                {
                    incoming_branch_merge
                        .defined_locally
                        .extend(incoming_branch.defined_locally);
                    incoming_branch_merge.alive.extend(incoming_branch.alive);
                    incoming_branch_merge.exclusive_defined_locally &=
                        incoming_branch.exclusive_defined_locally;
                } else {
                    incoming_branch_merge.exclusive_defined_locally = false;
                }
            }

            // backfill phis
            if let Some(backfill_phis_branch) = self.backfill_phis.remove(&branch) {
                self.backfill_phis
                    .entry(outer)
                    .or_default()
                    .extend(backfill_phis_branch);
            }
        }

        // update outer
        for node in node_updated_branch {
            self.node_updated.entry(outer).or_default().insert(node);
            let incoming_branch = incomings_branch.remove(&node).unwrap();
            let incoming_outer = self
                .node_definition
                .entry(node)
                .or_default()
                .entry(outer)
                .or_default();

            if incoming_branch.exclusive_defined_locally {
                // kill incomings of outer before
                // x = 1;       // killed, will never be used later
                // if (...) {
                //     x = 2;   // PhiIncoming 0
                // } else {
                //     x = 3;   // PhiIncoming 1
                // }
                incoming_outer.alive = incoming_outer
                    .alive
                    .difference(&incoming_outer.defined_locally)
                    .cloned()
                    .collect::<HashSet<_>>();
                incoming_outer
                    .alive
                    .extend(incoming_branch.defined_locally.clone());
                incoming_outer.defined_locally = incoming_branch.defined_locally;
                incoming_outer.exclusive_defined_locally = true;
                incoming_outer.exclusive_alive = true;
            } else {
                // merge incomings of outer before
                // x = 1;       // PhiIncoming 0
                // if (...) {
                //     x = 2;   // PhiIncoming 1
                // } else {
                //     ...;
                // }
                incoming_outer
                    .alive
                    .extend(incoming_branch.defined_locally.clone());
                incoming_outer
                    .defined_locally
                    .extend(incoming_branch.defined_locally);
            }
        }
    }

    fn fill_back_phis(&mut self, block: Pooled<BasicBlock>) {
        // x = 1;           // PhiIncoming 0
        // loop {
        //     use x;       // Phi(...)
        //     if (...) {
        //         x = 2;   // PhiIncoming 1
        //     }
        // } (...)

        let phis_to_backfill = self.backfill_phis.remove(&block).unwrap_or_default();
        for phi in phis_to_backfill {
            let var = *self.phi2var.get(&phi).unwrap();
            let incomings_body_end = self.node_definition.get(&var);
            if incomings_body_end.is_none() {
                continue;
            }
            let incomings_body_end = incomings_body_end.unwrap().get(&block);
            if incomings_body_end.is_none() {
                continue;
            }
            let incomings_body_end = incomings_body_end.unwrap();
            if incomings_body_end.defined_locally.is_empty() {
                continue;
            }
            match phi.get().instruction.as_ref() {
                Instruction::Phi(incomings) => {
                    println!("Before backfill: {:?}", phi);
                    let mut incomings: HashSet<_> = HashSet::from_iter(incomings.iter().cloned());
                    incomings.extend(incomings_body_end.defined_locally.clone());
                    let incomings =
                        CBoxedSlice::new(Vec::from_iter(incomings.iter().map(ToOwned::to_owned)));
                    phi.get_mut().instruction = CArc::new(Instruction::Phi(incomings));
                    println!("After backfill: {:?}", phi);
                }
                _ => panic!(),
            }
        }
    }

    fn process_block(&mut self, block: Pooled<BasicBlock>) {
        println!("Process block {:?}", block.ptr);

        let mut node = block.first.get().next;
        while node != block.last {
            let type_ = node.type_().as_ref();
            let instruction = node.get().instruction.as_ref();
            let node_next = node.get().next;
            println!("{:?}", node);
            match instruction {
                Instruction::Invalid => {
                    unreachable!("Invalid node should not appear in non-sentinel nodes");
                }
                Instruction::Buffer
                | Instruction::Bindless
                | Instruction::Texture2D
                | Instruction::Texture3D
                | Instruction::Accel
                | Instruction::Shared
                | Instruction::Uniform
                | Instruction::Argument { .. } => {
                    unreachable!("{:?} should not appear in basic block", instruction)
                }

                Instruction::Loop { body, cond } => {
                    let body = *body;
                    self.enter_block(body, block);
                    self.process_block(body);
                    // record use of cond. insert phi node to the end of body
                    let cond_new = self.record_use(body, *cond, body.last);

                    // backfill if in loop block
                    self.fill_back_phis(body);

                    self.exit_block(vec![body], block);

                    // Instruction replacement must be done last.
                    // Because when the former instruction is released, the instruction match reference
                    // will become "wild pointers"!
                    if cond_new != *cond {
                        node.get_mut().instruction = CArc::new(Instruction::Loop {
                            cond: cond_new,
                            body,
                        })
                    }
                }
                Instruction::If {
                    cond,
                    true_branch,
                    false_branch,
                } => {
                    let true_branch = *true_branch;
                    let false_branch = *false_branch;

                    // record use of cond
                    let cond_new = self.record_use(block, *cond, node);
                    self.enter_block(true_branch, block);
                    self.process_block(true_branch);
                    self.enter_block(false_branch, block);
                    self.process_block(false_branch);
                    self.exit_block(vec![true_branch, false_branch], block);

                    if cond_new != *cond {
                        node.get_mut().instruction = CArc::new(Instruction::If {
                            cond: cond_new,
                            true_branch,
                            false_branch,
                        })
                    }
                }
                Instruction::Switch {
                    value,
                    default,
                    cases,
                } => {
                    // record use of value
                    let value_new = self.record_use(block, *value, node);
                    let mut branches = Vec::new();
                    for case in cases.as_ref() {
                        self.enter_block(case.block, block);
                        self.process_block(case.block);
                        branches.push(case.block);
                    }
                    self.enter_block(*default, block);
                    self.process_block(*default);
                    branches.push(*default);
                    self.exit_block(branches, block);

                    if value_new != *value {
                        node.get_mut().instruction = CArc::new(Instruction::Switch {
                            value: value_new,
                            default: *default,
                            cases: cases.clone(),
                        })
                    }
                }
                Instruction::RayQuery {
                    ray_query,
                    on_triangle_hit,
                    on_procedural_hit,
                } => {
                    todo!();
                    self.record_use(block, *ray_query, node);
                    self.process_block(*on_triangle_hit);
                    self.process_block(*on_procedural_hit);
                    self.exit_block(vec![*on_triangle_hit, *on_procedural_hit], block);
                }
                Instruction::CoroRegister { token, value, var } => {
                    let value_new = self.record_use(block, *value, node);

                    if value_new != *value {
                        node.get_mut().instruction = CArc::new(Instruction::CoroRegister {
                            token: *token,
                            value: value_new,
                            var: *var,
                        })
                    }
                }

                Instruction::Local { init } => {
                    // record use of init
                    let mut value = self.record_use(block, *init, node);

                    if self.keep_unchanged(node) {
                        // keep this local node unchanged
                        if value != *init {
                            node.get_mut().instruction =
                                CArc::new(Instruction::Local { init: value });
                        }
                    } else {
                        // do not generate local node
                        node.remove();
                    }

                    // record def of Local node
                    self.record_def(block, node, value);
                }
                Instruction::Update { var, value } => {
                    // record use
                    let value_new = self.record_use(block, *value, node);
                    let var = *var;

                    // record def
                    self.record_def(block, var, value_new);

                    if self.keep_unchanged(var) {
                        // keep this update node unchanged
                        if value_new != *value {
                            node.get_mut().instruction = CArc::new(Instruction::Update {
                                var,
                                value: value_new,
                            });
                        }
                    } else {
                        // do not generate update node
                        node.remove();
                    }
                }
                Instruction::Const(_) => {
                    // record def
                    self.record_def(block, node, node);
                }
                Instruction::Call(func, args) => {
                    let mut args = args.clone();

                    // record use of args
                    let mut changed = false;
                    let mut args_new = Vec::new();
                    for arg in args.iter() {
                        let arg_new = self.record_use(block, *arg, node);
                        println!("arg = {:?}, arg_new = {:?}", *arg, arg_new);
                        args_new.push(arg_new);
                        if arg_new != *arg {
                            changed = true;
                        }
                    }
                    if changed {
                        args = CBoxedSlice::new(args_new);
                        node.get_mut().instruction =
                            CArc::new(Instruction::Call(func.clone(), args.clone()));
                    }

                    // record def of return value
                    // must replace node instruction before check keep_unchanged
                    // because we have updated args
                    let mut node_removed = false;
                    if !self.keep_unchanged(node) {
                        match func {
                            Func::Load => {
                                assert_eq!(args.len(), 1);
                                let arg = args[0];
                                println!("arg = {:?}", arg);
                                node.remove();
                                self.record_def(block, node, arg);
                                node_removed = true;
                            }
                            _ => unreachable!(),
                        }
                    }
                    if !node_removed {
                        self.record_def(block, node, node);
                    }
                }
                Instruction::Print { fmt, args } => {
                    let mut changed = false;
                    let mut args_new = Vec::new();
                    for arg in args.as_ref() {
                        let arg_new = self.record_use(block, *arg, node);
                        args_new.push(arg_new);
                        if arg_new != *arg {
                            changed = true;
                        }
                    }

                    if changed {
                        let args_new = CBoxedSlice::new(args_new);
                        node.get_mut().instruction = CArc::new(Instruction::Print {
                            fmt: fmt.clone(),
                            args: args_new,
                        });
                    }
                }
                Instruction::Phi(_) => {
                    unreachable!("Phi node unreachable before Mem2Reg pass");
                }

                Instruction::Return(value) => {
                    if value.valid() {
                        let value_new = self.record_use(block, *value, node);
                        if value_new != *value {
                            node.get_mut().instruction = CArc::new(Instruction::Return(value_new));
                        }
                    }
                }
                Instruction::GenericLoop { .. } | Instruction::Break | Instruction::Continue => {
                    unreachable!("{:?} should be lowered in CCF", instruction);
                }

                Instruction::AdScope { .. } | Instruction::AdDetach(_) => {
                    unimplemented!("{:?} unimplemented in Mem2Reg", instruction);
                }
                Instruction::UserData(_) => {}
                Instruction::Comment(_) => {}
                Instruction::CoroSplitMark { token } => {}
                Instruction::CoroSuspend { .. } | Instruction::CoroResume { .. } => {
                    unreachable!("{:?} unreachable in Mem2Reg", instruction);
                }
            }
            node = node_next;
        }
    }
}

pub struct Mem2Reg;

impl Mem2Reg {
    fn validate(module: &Module) {
        let nodes = collect_nodes(module.entry);
        for node in nodes {
            assert!(node.valid());
            match node.get().instruction.as_ref() {
                Instruction::If {
                    true_branch,
                    false_branch,
                    ..
                } => {
                    assert_ne!(true_branch.as_ptr(), false_branch.as_ptr());
                }
                Instruction::Invalid => {
                    unreachable!();
                }
                _ => {}
            }
        }
    }
}

impl Transform for Mem2Reg {
    fn transform_module(&self, module: Module) -> Module {
        Self::validate(&module);

        println!("{:-^40}", " Before Mem2Reg ");
        println!("{}", unsafe { DISPLAY_IR_DEBUG.get() }.display_ir(&module));

        let mut module = module;
        let mut impl_ = Mem2RegImpl::new(&module);
        impl_.pre_process();
        impl_.process_block(impl_.module_original);

        println!("{:-^40}", " After Mem2Reg ");
        println!("{}", DisplayIR::new().display_ir(&module));

        Self::validate(&module);

        module
    }
}
