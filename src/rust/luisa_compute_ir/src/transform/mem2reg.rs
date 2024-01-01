// This file implements the Mem2Reg pass to get an SSA form of the IR.
// We replace Local nodes' updates with Phi nodes.
// Reference paper: [2013] Simple and Efficient Construction of Static Single Assignment Form

use std::collections::{HashMap, HashSet};
use crate::ir::{ArrayType, BasicBlock, CallableModule, Func, Instruction, IrBuilder, MatrixType, Module, new_node, Node, NodeRef, PhiIncoming, Primitive, StructType, Type, VectorType};
use crate::{CArc, CBoxedSlice, Pooled};
use crate::analysis::utility::{DISPLAY_IR_DEBUG, is_primitives_read_only_function};
use crate::display::DisplayIR;
use crate::transform::Transform;

#[derive(Default, Clone)]
struct IncomingInfo {
    defined_locally: HashSet<PhiIncoming>,
    alive: HashSet<PhiIncoming>,
    exclusive: bool,
}

struct Mem2RegImpl {
    builder: IrBuilder,
    module_original: Pooled<BasicBlock>,

    node_definition: HashMap<NodeRef, HashMap<Pooled<BasicBlock>, IncomingInfo>>,
    node_updated: HashMap<Pooled<BasicBlock>, HashSet<NodeRef>>,
    node_by_ref: HashSet<NodeRef>,
}

impl Mem2RegImpl {
    fn new(module: &Module) -> Self {
        Self {
            builder: IrBuilder::new(module.pools.clone()),
            module_original: module.entry.clone(),
            node_definition: HashMap::new(),
            node_updated: HashMap::new(),
            node_by_ref: HashSet::new(),
        }
    }

    fn pre_process(&mut self) {
        self.scan_argument_by_ref(self.module_original);
    }
    fn scan_argument_by_ref(&mut self, block: Pooled<BasicBlock>) {
        let mut node = block.first.get().next;
        while node != block.last {
            let type_ = node.type_().as_ref();
            let instruction = node.get().instruction.as_ref();
            match instruction {
                Instruction::Call(func, args) => {
                    if !is_primitives_read_only_function(func, args) {
                        match func {
                            Func::Callable(callable) => {
                                for (parameter, arg) in callable.0.args.iter().zip(args.iter()) {
                                    if arg.is_primitive() && parameter.is_reference_argument() {
                                        self.node_by_ref.insert(*arg);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                // control flow
                Instruction::If { cond, true_branch, false_branch } => {
                    self.scan_argument_by_ref(*true_branch);
                    self.scan_argument_by_ref(*false_branch);
                }
                Instruction::Switch { value, default, cases } => {
                    for case in cases.as_ref() {
                        self.scan_argument_by_ref(case.block);
                    }
                    self.scan_argument_by_ref(*default);
                }
                Instruction::Loop { body, cond } => {
                    self.scan_argument_by_ref(*body);
                }
                _ => {}
            }
            node = node.get().next;
        }
    }

    fn record_def(&mut self, block: Pooled<BasicBlock>, var: NodeRef, value: NodeRef) {
        if !var.is_primitive() || self.node_by_ref.contains(&var) { return; }

        // some PhiIncomings are killed by this def
        let incomings = self.node_definition.entry(var).or_default().entry(block).or_default();
        incomings.alive = incomings.alive.difference(&incomings.defined_locally).cloned().collect::<HashSet<_>>();
        incomings.defined_locally.clear();

        // record new def
        let incoming = PhiIncoming {
            value,
            block,
        };
        incomings.defined_locally.insert(incoming);
        incomings.alive.insert(incoming);
        incomings.exclusive = true;
        self.node_updated.entry(block).or_default().insert(var);
    }
    /// Record use of a value.
    /// If a new phi node is created, return this phi node.
    /// Otherwise, return the original value.
    fn record_use(&mut self, block: Pooled<BasicBlock>, var: NodeRef, insert_before: NodeRef) -> NodeRef {
        if !var.is_primitive() || self.node_by_ref.contains(&var) { return var; }

        // record use
        let incomings = self.node_definition.get(&var).unwrap().get(&block).unwrap();
        assert!(incomings.exclusive);   // there must be a value for each incoming block
        let incomings_alive = CBoxedSlice::new(incomings.alive.iter().cloned().collect::<Vec<_>>());
        // Optimize if there is only 1 incoming
        if incomings_alive.len() == 1 {
            return incomings_alive[0].value;
        }
        let instruction = CArc::new(Instruction::Phi(incomings_alive));
        let phi = Node::new(instruction, var.type_().clone());
        let phi = new_node(
            &self.builder.pools,
            phi,
        );

        // insert phi node before ref_node
        insert_before.insert_before_self(phi);
        phi
    }
    fn enter_block(&mut self, branch: Pooled<BasicBlock>, outer: Pooled<BasicBlock>) {
        for (node, incoming_info) in self.node_definition.iter_mut() {
            if let Some(incoming_outer) = incoming_info.get(&outer) {
                let mut incoming_branch = incoming_outer.clone();
                incoming_branch.defined_locally.clear();
                assert!(incoming_info.insert(branch, incoming_branch).is_none());
            }
        }
        self.node_updated.entry(branch).or_default();
    }
    fn exit_block(&mut self, branches: Vec<Pooled<BasicBlock>>, outer: Pooled<BasicBlock>) {
        let mut node_updated_branch = HashSet::new();
        let mut incomings_branch: HashMap<NodeRef, IncomingInfo> = HashMap::new();

        for branch in branches {
            // add node updated by inner to outer
            let node_updated = self.node_updated.remove(&branch).unwrap();
            node_updated_branch.extend(node_updated.clone());

            for node in node_updated {
                let incoming_branch = self.node_definition.get_mut(&node).unwrap().remove(&branch).unwrap();
                let incoming_branch_merge = incomings_branch.entry(node).or_insert(
                    IncomingInfo {
                        defined_locally: HashSet::new(),
                        alive: HashSet::new(),
                        exclusive: true,
                    }
                );
                incoming_branch_merge.defined_locally.extend(incoming_branch.defined_locally);
                incoming_branch_merge.alive.extend(incoming_branch.alive);
                incoming_branch_merge.exclusive &= incoming_branch.exclusive;
            }
        }

        // update outer
        for node in node_updated_branch {
            self.node_updated.entry(outer).or_default().insert(node);
            let incoming_branch = incomings_branch.remove(&node).unwrap();
            let incoming_outer = self.node_definition.entry(node).or_default().entry(outer).or_default();

            if incoming_branch.exclusive {
                // kill incomings of outer before
                // x = 1;       // killed, will never be used later
                // if (...) {
                //     x = 2;   // PhiIncoming 0
                // } else {
                //     x = 3;   // PhiIncoming 1
                // }
                incoming_outer.alive = incoming_outer.alive.difference(&incoming_outer.defined_locally).cloned().collect::<HashSet<_>>();
                incoming_outer.alive.extend(incoming_branch.defined_locally.clone());
                incoming_outer.defined_locally = incoming_branch.defined_locally;
            } else {
                // merge incomings of outer before
                // x = 1;       // PhiIncoming 0
                // if (...) {
                //     x = 2;   // PhiIncoming 1
                // } else {
                //     ...;
                // }
                incoming_outer.alive.extend(incoming_branch.defined_locally.clone());
                incoming_outer.defined_locally.extend(incoming_branch.defined_locally);
            }
            incoming_outer.exclusive = incoming_outer.exclusive || incoming_branch.exclusive;
        }
    }

    fn process_block(&mut self, block: Pooled<BasicBlock>) {
        let mut node = block.first.get().next;
        while node != block.last {
            let type_ = node.type_().as_ref();
            let instruction = node.get().instruction.as_ref();
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
                    self.enter_block(*body, block);
                    self.process_block(*body);
                    // record use of cond. insert phi node to the end of body
                    self.record_use(*body, *cond, body.last);
                    self.exit_block(vec![*body], block);
                }
                Instruction::If {
                    cond,
                    true_branch,
                    false_branch
                } => {
                    // record use of cond
                    self.record_use(block, *cond, node);
                    self.enter_block(*true_branch, block);
                    self.process_block(*true_branch);
                    self.enter_block(*false_branch, block);
                    self.process_block(*false_branch);
                    self.exit_block(vec![*true_branch, *false_branch], block);
                }
                Instruction::Switch {
                    value,
                    default,
                    cases
                } => {
                    // record use of value
                    self.record_use(block, *value, node);
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
                }
                Instruction::RayQuery {
                    ray_query,
                    on_triangle_hit,
                    on_procedural_hit
                } => {
                    todo!();
                    self.record_use(block, *ray_query, node);
                    self.process_block(*on_triangle_hit);
                    self.process_block(*on_procedural_hit);
                    self.exit_block(vec![*on_triangle_hit, *on_procedural_hit], block);
                }
                Instruction::CoroRegister { token, value, var } => {
                    self.record_use(block, *value, node);
                }

                Instruction::Local { init } => {
                    // record use of init
                    let init_new = self.record_use(block, *init, node);
                    if init_new != *init {
                        node.get_mut().instruction = CArc::new(Instruction::Local {
                            init: init_new,
                        });
                    }

                    // record def of Local node
                    self.record_def(block, node, *init);
                }
                Instruction::Update { var, value } => {
                    // record use
                    let value_new = self.record_use(block, *value, node);
                    if value_new != *value {
                        node.get_mut().instruction = CArc::new(Instruction::Update {
                            var: *var,
                            value: value_new,
                        });
                    }

                    // record def
                    self.record_def(block, *var, *value);
                }
                Instruction::Const(_) => {
                    // record def
                    self.record_def(block, node, node);
                }
                Instruction::Call(func, args) => {
                    // record def of return value
                    self.record_def(block, node, node);
                    // arguments by ref have been skipped in pre_process
                }
                Instruction::Print { fmt, args} => {
                    for arg in args.as_ref() {
                        self.record_use(block, *arg, node);
                    }
                }
                Instruction::Phi(_) => {
                    unreachable!("Phi node unreachable before Mem2Reg pass");
                }

                Instruction::Return(value) => {
                    if value.valid() {
                        self.record_use(block, *value, node);
                    }
                }
                Instruction::GenericLoop { .. }
                | Instruction::Break
                | Instruction::Continue => {
                    unreachable!("{:?} should be lowered in CCF", instruction);
                }

                Instruction::AdScope { .. }
                | Instruction::AdDetach(_) => {
                    unimplemented!("{:?} unimplemented in Mem2Reg", instruction);
                }
                Instruction::UserData(_) => {}
                Instruction::Comment(_) => {}
                Instruction::CoroSplitMark { .. } => {}
                Instruction::CoroSuspend { .. }
                | Instruction::CoroResume { .. } => {
                    unreachable!("{:?} unreachable in Mem2Reg", instruction);
                }
            }
            node = node.get().next;
        }
    }
}

pub struct Mem2Reg;

impl Transform for Mem2Reg {
    fn transform_callable(&self, module: CallableModule) -> CallableModule {
        println!("{:-^40}", " Before Mem2Reg ");
        println!("{}", DisplayIR::new().display_ir_callable(&module));

        let mut module = module;
        let mut impl_ = Mem2RegImpl::new(&module.module);
        impl_.pre_process();
        impl_.process_block(impl_.module_original);

        println!("{:-^40}", " After Mem2Reg ");
        println!("{}", DisplayIR::new().display_ir_callable(&module));

        module
    }
}