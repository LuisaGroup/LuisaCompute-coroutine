use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::process::exit;
use sha2::digest::DynDigest;
use crate::analysis::coro_frame_v4::{CoroFrameAnalyser, CoroFrameAnalysis};
// use crate::analysis::coro_graph::CoroPreliminaryGraph;
use crate::analysis::coro_graph::{CoroGraph, CoroInstrRef, CoroInstruction, CoroScopeRef};
use crate::{CArc, CBoxedSlice, Pooled};
use crate::analysis::frame_token_manager::INVALID_FRAME_TOKEN_MASK;
use crate::analysis::utility::{DISPLAY_IR_DEBUG, node_updatable};
use crate::context::register_type;
use crate::display::DisplayIR;
use crate::ir::{BasicBlock, CallableModule, CallableModuleRef, Capture, Const, Func, Instruction, INVALID_REF, IrBuilder, Module, ModuleFlags, ModuleKind, ModulePools, new_node, Node, NodeRef, PhiIncoming, Primitive, SwitchCase, Type};
use crate::transform::Transform;


#[derive(Clone)]
struct VisitState {
    bb: Vec<CoroInstrRef>,
    instr_index: usize,
}

struct ScopeBuilder {
    token: u32,
    builder: IrBuilder,
}

impl ScopeBuilder {
    fn new(frame_token: u32, pools: CArc<ModulePools>) -> Self {
        Self {
            token: frame_token,
            builder: IrBuilder::new(pools),
        }
    }
}

struct CallableModuleInfo {
    args: Vec<NodeRef>,
    captures: Vec<Capture>,
    frame_node: NodeRef,
    old2frame_index: HashMap<NodeRef, usize>,
    register_var2index: HashMap<u32, usize>,
    flag_ref2node_ref: HashMap<CoroInstrRef, NodeRef>,
    condition_stack_replay: HashMap<NodeRef, NodeRef>,
}

#[derive(Default)]
struct Old2NewMap {
    callables: HashMap<*const CallableModule, HashMap<u32, CArc<CallableModule>>>,
    nodes: HashMap<NodeRef, HashMap<u32, NodeRef>>,
    blocks: HashMap<*const BasicBlock, HashMap<u32, Pooled<BasicBlock>>>,
}

#[derive(Default)]
struct New2OldMap {
    callables: HashMap<*const CallableModule, *const CallableModule>,
    nodes: HashMap<NodeRef, NodeRef>,
    blocks: HashMap<*const BasicBlock, *const BasicBlock>,
}


struct SplitManager {
    callable: CallableModule,
    graph: CoroGraph,
    frame_analyser: CoroFrameAnalyser,

    new2old: New2OldMap,
    old2new: Old2NewMap,

    coro_scopes: HashMap<u32, Pooled<BasicBlock>>,
    replayable_builders: HashMap<u32, IrBuilder>,
    coro_callable_info: HashMap<u32, CallableModuleInfo>,
}

impl SplitManager {
    fn split(callable: CallableModule, graph: CoroGraph, frame_analyser: CoroFrameAnalyser) -> CallableModule {
        let mut manager = Self {
            callable,
            graph,
            frame_analyser,
            new2old: New2OldMap::default(),
            old2new: Old2NewMap::default(),
            coro_scopes: HashMap::new(),
            replayable_builders: HashMap::new(),
            coro_callable_info: HashMap::new(),
        };
        manager.pre_process();

        macro_rules! generate_code {
            ($token: expr, $scope_ref: expr, $coro_resume: expr) => {{
                let bb = manager.graph.scopes.get($scope_ref.0).unwrap().instructions.clone();
                let visit_state = VisitState {
                    bb,
                    instr_index: 0,
                };
                let mut scope_builder = ScopeBuilder::new($token, manager.callable.pools.clone());
                if $coro_resume {
                    scope_builder = manager.coro_resume(scope_builder);
                }
                let scope_builder = manager.visit_bb(
                    scope_builder,
                    visit_state);
                let bb = scope_builder.builder.finish();
                let mut replayable_builder = manager.replayable_builders.remove(&$token).unwrap();
                replayable_builder.append_block(bb);
                let bb = replayable_builder.finish();
                manager.coro_scopes.insert($token, bb);
            }};
        }

        let entry_token = manager.frame_analyser.entry_token;
        generate_code!(entry_token, manager.graph.entry, false);

        let sub_coros = manager.graph.tokens.clone();
        for (token, scope_ref) in sub_coros.iter() {
            generate_code!(*token, *scope_ref, true);
        }

        manager.build_coroutines()
    }

    fn build_coroutines(&self) -> CallableModule {
        let entry_token = self.frame_analyser.entry_token;
        let mut subroutines = vec![];
        let mut subroutine_ids = vec![];
        let callable = &self.callable;
        for (token, callable_info) in self.coro_callable_info.iter() {
            if *token == entry_token {
                continue;
            }
            let scope = self.coro_scopes.get(token).unwrap();
            let coroutine = CallableModule {
                module: Module {
                    kind: ModuleKind::Function,
                    entry: *scope,
                    flags: ModuleFlags::NONE,   // TODO: auto diff not allowed
                    curve_basis_set: callable.module.curve_basis_set.clone(),
                    pools: callable.pools.clone(),
                },
                ret_type: register_type(Type::Void),    // TODO: coroutine return type: only void allowed
                args: CBoxedSlice::from(callable_info.args.as_slice()),
                captures: CBoxedSlice::from(callable_info.captures.as_slice()),
                subroutines: CBoxedSlice::new(vec![]),
                subroutine_ids: CBoxedSlice::new(vec![]),
                cpu_custom_ops: callable.cpu_custom_ops.clone(),
                pools: callable.pools.clone(),
            };
            let coroutine = CallableModuleRef(CArc::new(coroutine));
            subroutines.push(coroutine);
            subroutine_ids.push(*token);
        }
        {
            let callable_info = self.coro_callable_info.get(&entry_token).unwrap();
            let scope = self.coro_scopes.get(&entry_token).unwrap();
            CallableModule {
                module: Module {
                    kind: ModuleKind::Function,
                    entry: *scope,
                    flags: ModuleFlags::NONE,   // TODO: auto diff not allowed
                    curve_basis_set: callable.module.curve_basis_set.clone(),
                    pools: callable.pools.clone(),
                },
                ret_type: register_type(Type::Void),    // TODO: coroutine return type: only void allowed
                args: CBoxedSlice::from(callable_info.args.as_slice()),
                captures: CBoxedSlice::from(callable_info.captures.as_slice()),
                subroutines: CBoxedSlice::from(subroutines.as_slice()),
                subroutine_ids: CBoxedSlice::from(subroutine_ids.as_slice()),
                cpu_custom_ops: callable.cpu_custom_ops.clone(),
                pools: callable.pools.clone(),
            }
        }
    }

    fn pre_process(&mut self) {
        let token_vec = self.frame_analyser.token_vec();
        let mut coro_callable_info = HashMap::new();
        let captures = self.callable.captures.clone();
        let args = self.callable.args.clone();
        for token in token_vec.iter() {
            let args = self.duplicate_args(*token, &args);
            let captures = self.duplicate_captures(*token, &captures);
            // args[0] is frame by default
            // replace the type of frame
            args[0].get_mut().type_ = self.frame_analyser.frame_type.clone();
            let mut callable_info = CallableModuleInfo {
                args,
                captures,
                frame_node: INVALID_REF,    // set later
                old2frame_index: self.frame_analyser.node2frame_slot.clone(),
                register_var2index: HashMap::new(),
                flag_ref2node_ref: HashMap::new(),
                condition_stack_replay: HashMap::new(),
            };
            callable_info.frame_node = callable_info.args[0].clone();

            coro_callable_info.insert(*token, callable_info);
            self.replayable_builders.insert(*token, IrBuilder::new(self.callable.pools.clone()));
        }
        self.coro_callable_info = coro_callable_info;
    }

    fn empty_block(pools: CArc<ModulePools>) -> Pooled<BasicBlock> {
        let mut builder = IrBuilder::new(pools);
        builder.finish()
    }

    fn visit_bb(
        &mut self,
        mut scope_builder: ScopeBuilder,
        visit_state: VisitState,
    ) -> ScopeBuilder {
        let token = scope_builder.token;

        macro_rules! find_cond {
            ($cond: expr) => {{
                let cond = self.graph.instructions.get($cond.0).unwrap();
                let cond_new = if let CoroInstruction::Simple(cond) = cond {
                    self.find_duplicated_node(token, *cond)
                } else {
                    unreachable!("Cond must be a simple node")
                };
                cond_new
            }};
        }

        macro_rules! visit_branch {
            ($branch: expr) => {{
                let visit_state = VisitState {
                    bb: $branch,
                    instr_index: 0,
                };
                let builder = ScopeBuilder::new(token, self.callable.pools.clone());
                let builder = self.visit_bb(builder, visit_state);
                let bb = builder.builder.finish();
                bb
            }};
        }

        let mut visit_state = visit_state;
        let bb = &visit_state.bb;
        while visit_state.instr_index < bb.len() {
            let coro_instr_ref = bb.get(visit_state.instr_index).unwrap().clone();
            let coro_instr = self.graph.instructions.get(coro_instr_ref.0).unwrap().clone();
            // println!("{:?}", coro_instr);

            match &coro_instr {
                CoroInstruction::Entry
                | CoroInstruction::EntryScope { .. } => {
                    unreachable!("{:?} shouldn't exist after coro graph analysis", coro_instr);
                }
                CoroInstruction::ConditionStackReplay { items } => {
                    for item in items.iter() {
                        let node = item.node;
                        let value = item.value;
                        let type_ = node.type_().as_ref();
                        println!("ConditionStackReplay {:?}: {:?} = {:?}", node, type_, value);
                        let value = {
                            let builder = self.replayable_builders.get_mut(&token).unwrap();
                            match type_ {
                                &Type::Primitive(Primitive::Bool) => builder.const_(Const::Bool(value == 1)),
                                &Type::Primitive(Primitive::Int32) => builder.const_(Const::Int32(value)),
                                _ => panic!("Unexpected type in ConditionStackReplay"),
                            }
                        };
                        if node_updatable(node) {
                            scope_builder.builder.update(node, value);
                        } else {
                            // set a new node to replace the old one
                            self.coro_callable_info.get_mut(&token).unwrap().condition_stack_replay.insert(node, value);
                        }
                    }
                }

                CoroInstruction::MakeFirstFlag => {
                    // create a cond node and record the CoroInstrRef -> NodeRef mapping
                    let flag = scope_builder.builder.local_zero_init(register_type(Type::Primitive(Primitive::Bool)));
                    let true_value = scope_builder.builder.const_(Const::Bool(true));
                    scope_builder.builder.update(flag, true_value);
                    self.coro_callable_info.get_mut(&token).unwrap().flag_ref2node_ref.insert(coro_instr_ref, flag);
                }
                CoroInstruction::SkipIfFirstFlag { flag, body } => {
                    // find the NodeRef cond
                    let cond = *self.coro_callable_info.get(&token).unwrap().flag_ref2node_ref.get(flag).unwrap();
                    let body_new = visit_branch!(body.clone());
                    scope_builder.builder.if_(cond, Self::empty_block(self.callable.pools.clone()), body_new);
                }
                CoroInstruction::ClearFirstFlag(flag) => {
                    // set the flag to false
                    let cond = self.coro_callable_info.get(&token).unwrap().flag_ref2node_ref.get(flag).unwrap();
                    let false_value = scope_builder.builder.const_(Const::Bool(false));
                    scope_builder.builder.update(*cond, false_value);
                }
                CoroInstruction::Suspend { token: token_next } => {
                    // store coro states
                    scope_builder.builder.coro_suspend(*token_next);
                    self.coro_store(&mut scope_builder, *token_next);
                    // create a void type node for termination
                    scope_builder.builder.return_(INVALID_REF);
                }

                CoroInstruction::Simple(node) => {
                    let type_ = node.type_().as_ref();
                    let instruction = node.get().instruction.as_ref();

                    let dup_node = if let Some(value) =
                        self.coro_callable_info.get(&token).unwrap().condition_stack_replay.get(node) {
                        // create a local
                        let replayable_builder = self.replayable_builders.get_mut(&token).unwrap();
                        replayable_builder.comment(CBoxedSlice::from("ConditionStackReplay".as_bytes()));
                        let dup_node = replayable_builder.local(*value);
                        // update value
                        let node_original = new_node(
                            &self.callable.pools,
                            Node::new(node.get().instruction.clone(), node.type_().clone()));
                        scope_builder.builder.append(node_original);
                        scope_builder.builder.update(dup_node, node_original);
                        dup_node
                    } else {
                        let dup_node = match instruction {
                            Instruction::CoroSplitMark { .. }
                            | Instruction::CoroSuspend { .. }
                            | Instruction::CoroResume { .. } => {
                                unreachable!("{:?} does not belong to CoroInstruction::Simple", instruction)
                            }
                            Instruction::CoroRegister { token, value, var } => {
                                // var <-> frame[token][index] <-> value
                                assert_eq!(*token, scope_builder.token);
                                let callable_info = self.coro_callable_info.get_mut(token).unwrap();
                                let index = callable_info.old2frame_index.get(value).unwrap();
                                callable_info.register_var2index.insert(*var, *index);
                                INVALID_REF
                            }

                            Instruction::Loop { .. }
                            | Instruction::GenericLoop { .. }
                            | Instruction::Break
                            | Instruction::Continue
                            | Instruction::Return(_)
                            | Instruction::If { .. }
                            | Instruction::Switch { .. } => {
                                unreachable!("{:?} does not belong to CoroInstruction::Simple", instruction);
                            }

                            _ => self.duplicate_node(&mut scope_builder, *node),
                        };
                        dup_node
                    };
                    if dup_node != INVALID_REF {
                        self.record_node_mapping(token, *node, dup_node);
                    }
                }

                // 3 Instructions after CCF
                CoroInstruction::Loop { body, cond } => {
                    let body_new = visit_branch!(body.clone());
                    let cond_new = find_cond!(cond);
                    scope_builder.builder.loop_(body_new, cond_new);
                }
                CoroInstruction::If {
                    cond,
                    true_branch,
                    false_branch,
                } => {
                    let cond_new = find_cond!(cond);
                    let body_true = visit_branch!(true_branch.clone());
                    let body_false = visit_branch!(false_branch.clone());
                    scope_builder.builder.if_(cond_new, body_true, body_false);
                }
                CoroInstruction::Switch {
                    cond,
                    cases,
                    default,
                } => {
                    let cond_new = find_cond!(cond);
                    let cases_new: Vec<_> = cases.iter().map(|case| {
                        let body_new = visit_branch!(case.body.clone());
                        SwitchCase {
                            value: case.value,
                            block: body_new,
                        }
                    }).collect();
                    let defalt_new = visit_branch!(default.clone());
                    scope_builder.builder.switch(cond_new, cases_new.as_slice(), defalt_new);
                }

                CoroInstruction::Terminate => {
                    scope_builder.builder.coro_suspend(INVALID_FRAME_TOKEN_MASK);
                    self.coro_store(&mut scope_builder, INVALID_FRAME_TOKEN_MASK);
                    scope_builder.builder.return_(INVALID_REF);
                }
            }
            visit_state.instr_index += 1;
        }
        scope_builder
    }

    fn coro_resume(&mut self, mut scope_builder: ScopeBuilder) -> ScopeBuilder {
        let token = scope_builder.token;
        let builder = &mut scope_builder.builder;
        builder.comment(CBoxedSlice::from("CoroResume Start".as_bytes())); // for DEBUG
        let old2frame_index = &self.coro_callable_info.get(&token).unwrap().old2frame_index.clone();
        let frame_node = self.coro_callable_info.get(&token).unwrap().frame_node;
        for (old_node, index) in old2frame_index.iter() {
            let index_node = builder.const_(Const::Uint32(*index as u32));
            let gep = builder.gep_chained(
                frame_node, &[index_node], self.frame_analyser.frame_fields[*index].clone());
            self.record_node_mapping(token, *old_node, gep);
        }
        builder.comment(CBoxedSlice::from("CoroResume End".as_bytes())); // for DEBUG
        scope_builder
    }
    fn coro_store(&mut self, scope_builder: &mut ScopeBuilder, token_next: u32) {
        let token = scope_builder.token;
        let builder = &mut scope_builder.builder;
        builder.comment(CBoxedSlice::from("CoroSuspend Start".as_bytes())); // for DEBUG

        let frame_node = self.coro_callable_info.get(&token).unwrap().frame_node;
        let valid_token = token_next & INVALID_FRAME_TOKEN_MASK == 0;
        if valid_token {
            // store frame state
            let old2frame_index = &self.coro_callable_info.get(&token_next).unwrap().old2frame_index;
            let frame_fields = self.frame_analyser.frame_fields.clone(); // clone to avoid multiple borrow
            let output_vars = self.frame_analyser.output_vars(token, token_next);
            for old_node in output_vars.iter() {
                println!("{:?}", old_node);
                unsafe { println!("{}", DISPLAY_IR_DEBUG.get().var_str(old_node)); }
                let index = old2frame_index.get(old_node).unwrap();

                let index_node = builder.const_(Const::Uint32(*index as u32));
                let old2new_map = self.old2new.nodes.get(old_node).unwrap();
                let value = old2new_map.get(&token).unwrap().clone();
                let value = if value.is_lvalue() {
                    builder.load(value)
                } else {
                    value
                };
                let gep = builder.gep_chained(
                    frame_node,
                    &[index_node],
                    frame_fields[*index].clone(),
                );
                builder.update(gep, value);
            }
        }

        // change frame token
        let index_node = builder.const_(Const::Uint32(crate::analysis::coro_frame_v4::STATE_INDEX_FRAME_TOKEN as u32));
        let gep = builder.gep_chained(
            frame_node,
            &[index_node],
            self.frame_analyser.frame_fields[crate::analysis::coro_frame_v4::STATE_INDEX_FRAME_TOKEN].clone(),
        );
        let value = builder.const_(Const::Uint32(token_next));
        builder.update(gep, value);

        builder.comment(CBoxedSlice::from("CoroSuspend End".as_bytes())); // for DEBUG
    }

    // duplicate functions
    fn find_duplicated_block(
        &mut self,
        frame_token: u32,
        bb: &Pooled<BasicBlock>,
    ) -> Pooled<BasicBlock> {
        self.old2new
            .blocks
            .get(&bb.as_ptr())
            .unwrap()
            .get(&frame_token)
            .unwrap()
            .clone()
    }
    fn find_duplicated_node(&mut self, frame_token: u32, node: NodeRef) -> NodeRef {
        if !node.valid() {
            return INVALID_REF;
        }

        if let Some(new) = self.old2new.nodes.get(&node).unwrap().get(&frame_token) {
            // already been duplicated
            new.clone()
        } else {
            // not been duplicated
            if self.frame_analyser.replayable_value_analysis.detect(node) {
                // replayable nodes
                self.duplicate_replayable(frame_token, node)
            } else {
                panic!("Unreplayable node not found in find_duplicated_node => {:?}: {:?} = {:?}",
                       unsafe { DISPLAY_IR_DEBUG.get().var_str(&node) }, node.type_(), node);
            }
        }
    }
    fn duplicate_replayable(&mut self, frame_token: u32, node: NodeRef) -> NodeRef {
        let mut builder = ScopeBuilder::new(frame_token, self.callable.pools.clone());

        builder.builder.comment(CBoxedSlice::from("Replayable node".as_bytes()));
        let node = self.duplicate_node(&mut builder, node);
        let bb = builder.builder.finish();
        let replayable_builder = self.replayable_builders.get_mut(&frame_token).unwrap();
        replayable_builder.append_block(bb);
        node
    }
    fn duplicate_block(
        &mut self,
        frame_token: u32,
        pools: &CArc<ModulePools>,
        bb: &Pooled<BasicBlock>,
    ) -> Pooled<BasicBlock> {
        assert!(!self.old2new.blocks.entry(bb.as_ptr()).or_default().contains_key(&frame_token),
                "Frame token {}, basic block {:?} has already been duplicated", frame_token, bb);
        let mut scope_builder = ScopeBuilder::new(frame_token, pools.clone());
        for node in bb.iter() {
            // println!("Token {}, Visit noderef {:?} : {:?}", scope_builder.token, node.0, node.get().instruction);
            self.duplicate_node(&mut scope_builder, node);
            match node.get().instruction.as_ref() {
                Instruction::CoroSuspend { .. } => {
                    break;
                }
                _ => {}
            }
        }
        let dup_bb = scope_builder.builder.finish();
        // insert the duplicated block into the map
        self.record_block_mapping(frame_token, bb, &dup_bb);
        dup_bb
    }
    fn duplicate_module(&mut self, frame_token: u32, module: &Module) -> Module {
        let dup_entry = self.duplicate_block(frame_token, &module.pools, &module.entry);
        Module {
            kind: module.kind.clone(),
            entry: dup_entry,
            flags: module.flags.clone(),
            pools: module.pools.clone(),
            curve_basis_set: module.curve_basis_set.clone(),
        }
    }
    fn duplicate_callable(
        &mut self,
        frame_token: u32,
        callable: &CArc<CallableModule>,
    ) -> CArc<CallableModule> {
        let pools = &callable.pools;
        if let Some(copy) =
            self.old2new.callables.get(&callable.as_ptr()).unwrap().get(&frame_token) {
            return copy.clone();
        }
        let dup_callable = {
            let dup_args = self.duplicate_args(frame_token, &callable.args);
            let dup_captures = self.duplicate_captures(frame_token, &callable.captures);
            let dup_module = self.duplicate_module(frame_token, &callable.module);
            CallableModule {
                module: dup_module,
                ret_type: callable.ret_type.clone(),
                args: CBoxedSlice::new(dup_args),
                captures: CBoxedSlice::new(dup_captures),
                subroutines: callable.subroutines.clone(),
                subroutine_ids: callable.subroutine_ids.clone(),
                cpu_custom_ops: callable.cpu_custom_ops.clone(),
                pools: pools.clone(),
            }
        };
        let dup_callable = CArc::new(dup_callable);
        // insert the duplicated callable into the map
        self.record_callable_mapping(frame_token, callable, &dup_callable);
        dup_callable
    }
    fn duplicate_node(
        &mut self,
        mut scope_builder: &mut ScopeBuilder,
        node_ref: NodeRef,
    ) -> NodeRef {
        if !node_ref.valid() {
            return INVALID_REF;
        }
        let frame_token = scope_builder.token;
        let node = node_ref.get();
        // // TODO: for DEBUG
        // if self.old2new.nodes.entry(node_ref).or_default().contains_key(&frame_token) {
        //     println!("\n{:?}\n", self.old2new.nodes.get(&node_ref).unwrap());
        //     panic!("Frame token {}, noderef {:?}, node {:?} has already been duplicated", frame_token, node_ref, node);
        // }
        // // FIXME: multiple duplication in loop transformation
        // assert!(!self.old2new.nodes.entry(node_ref).or_default().contains_key(&frame_token),
        //         "Frame token {}, node {:?} has already been duplicated", frame_token, node);
        let instruction = node.instruction.as_ref();
        let dup_node = match instruction {
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
            Instruction::Invalid => {
                unreachable!("Invalid node should not appear in non-sentinel nodes")
            }

            Instruction::Local { init } => {
                let dup_init = self.find_duplicated_node(frame_token, *init);
                scope_builder.builder.local(dup_init)
            }
            Instruction::UserData(data) => scope_builder.builder.userdata(data.clone()),
            Instruction::Const(const_) => scope_builder.builder.const_(const_.clone()),
            Instruction::Update { var, value } => {
                // unreachable if SSA
                let dup_var = self.find_duplicated_node(frame_token, *var);
                let dup_value = self.find_duplicated_node(frame_token, *value);
                scope_builder.builder.update(dup_var, dup_value)
            }
            Instruction::Call(func, args) => {
                let dup_func = match func {
                    Func::Callable(callable) => {
                        let dup_callable = self.duplicate_callable(frame_token, &callable.0);
                        Func::Callable(CallableModuleRef(dup_callable))
                    }
                    _ => func.clone(),
                };
                let mut dup_args: Vec<_> = args
                    .iter()
                    .map(|arg| {
                        let dup_arg = self.find_duplicated_node(frame_token, *arg);
                        dup_arg
                    })
                    .collect();
                if func == &Func::CoroId || func == &Func::CoroToken {
                    assert!(
                        dup_args.is_empty(),
                        "Args of function {:?} is not empty",
                        func
                    );
                    let frame_node = self.coro_callable_info.get(&frame_token).unwrap().frame_node;
                    dup_args.push(frame_node);
                }
                scope_builder.builder.call(dup_func, dup_args.as_slice(), node.type_.clone())
            }
            Instruction::Phi(incomings) => {
                let dup_incomings: Vec<_> = incomings
                    .iter()
                    .map(|incoming| {
                        let dup_block = self.find_duplicated_block(frame_token, &incoming.block);
                        let dup_value = self.find_duplicated_node(frame_token, incoming.value);
                        PhiIncoming {
                            value: dup_value,
                            block: dup_block,
                        }
                    })
                    .collect();
                scope_builder.builder.phi(dup_incomings.as_slice(), node.type_.clone())
            }
            Instruction::AdScope { body, .. } => {
                let dup_body =
                    self.duplicate_block(frame_token, &scope_builder.builder.pools, body);
                scope_builder.builder.ad_scope(dup_body)
            }
            Instruction::RayQuery {
                ray_query,
                on_triangle_hit,
                on_procedural_hit,
            } => {
                let dup_ray_query = self.find_duplicated_node(frame_token, *ray_query);
                let dup_on_triangle_hit = self.duplicate_block(frame_token, &scope_builder.builder.pools, on_triangle_hit);
                let dup_on_procedural_hit = self.duplicate_block(frame_token, &scope_builder.builder.pools, on_procedural_hit);
                scope_builder.builder.ray_query(
                    dup_ray_query,
                    dup_on_triangle_hit,
                    dup_on_procedural_hit,
                    node.type_.clone(),
                )
            }
            Instruction::AdDetach(body) => {
                let dup_body = self.duplicate_block(frame_token, &scope_builder.builder.pools, body);
                scope_builder.builder.ad_detach(dup_body)
            }
            Instruction::Comment(msg) => scope_builder.builder.comment(msg.clone()),

            Instruction::Return(value) => {
                let dup_value = self.find_duplicated_node(frame_token, *value);
                scope_builder.builder.return_(dup_value)
            }

            Instruction::GenericLoop { .. } | Instruction::Break | Instruction::Continue => {
                unreachable!("{:?} should be handled in CCF", instruction)
            }

            Instruction::Loop { body, cond } => {
                let dup_body =
                    self.duplicate_block(frame_token, &scope_builder.builder.pools, body);
                let dup_cond = self.find_duplicated_node(frame_token, *cond);
                scope_builder.builder.loop_(dup_body, dup_cond)
            }
            Instruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                let dup_cond = self.find_duplicated_node(frame_token, *cond);
                let dup_true_branch =
                    self.duplicate_block(frame_token, &scope_builder.builder.pools, true_branch);
                let dup_false_branch =
                    self.duplicate_block(frame_token, &scope_builder.builder.pools, false_branch);
                scope_builder.builder.if_(dup_cond, dup_true_branch, dup_false_branch)
            }
            Instruction::Switch {
                value,
                cases,
                default,
            } => {
                let dup_value = self.find_duplicated_node(frame_token, *value);
                let dup_cases: Vec<_> = cases
                    .iter()
                    .map(|case| {
                        let dup_block = self.duplicate_block(
                            frame_token,
                            &scope_builder.builder.pools,
                            &case.block,
                        );
                        SwitchCase {
                            value: case.value,
                            block: dup_block,
                        }
                    })
                    .collect();
                let dup_default =
                    self.duplicate_block(frame_token, &scope_builder.builder.pools, default);
                scope_builder.builder.switch(dup_value, dup_cases.as_slice(), dup_default)
            }

            Instruction::Print { fmt, args } => {
                let args = args
                    .iter()
                    .map(|x| self.find_duplicated_node(frame_token, *x))
                    .collect::<Vec<_>>();
                scope_builder.builder.print(fmt.clone(), &args)
            }

            // Coro
            Instruction::CoroSuspend { .. }
            | Instruction::CoroRegister { .. }
            | Instruction::CoroSplitMark { .. }
            | Instruction::CoroResume { .. } => unreachable!(
                "Unexpected instruction {:?} in SplitManager::duplicate_node",
                instruction
            ),
        };
        // insert the duplicated node into the map
        self.record_node_mapping(frame_token, node_ref, dup_node);
        dup_node
    }
    fn duplicate_arg(&mut self, frame_token: u32, node_ref: NodeRef) -> NodeRef {
        let node = node_ref.get();
        let instr = &node.instruction;
        let dup_instr = match instr.as_ref() {
            Instruction::Buffer => instr.clone(),
            Instruction::Bindless => instr.clone(),
            Instruction::Texture2D => instr.clone(),
            Instruction::Texture3D => instr.clone(),
            Instruction::Accel => instr.clone(),
            Instruction::Shared => instr.clone(),
            Instruction::Uniform => instr.clone(),
            Instruction::Argument { .. } => CArc::new(instr.as_ref().clone()),
            _ => unreachable!("Invalid argument type"),
        };
        let dup_node = Node::new(dup_instr, node.type_.clone());
        let dup_node_ref = new_node(&self.callable.pools, dup_node);

        // add to node map
        self.record_node_mapping(frame_token, node_ref, dup_node_ref);
        dup_node_ref
    }
    fn duplicate_args(&mut self, frame_token: u32, args: &CBoxedSlice<NodeRef>) -> Vec<NodeRef> {
        let dup_args: Vec<NodeRef> = args
            .iter()
            .map(|arg| {
                let dup_arg = self.duplicate_arg(frame_token, *arg);
                // println!("{:?} => {:?}", arg, dup_arg);  // for DEBUG
                dup_arg
            })
            .collect();
        dup_args
    }
    fn duplicate_capture(&mut self, frame_token: u32, capture: &Capture) -> Capture {
        Capture {
            node: self.duplicate_arg(frame_token, capture.node),
            binding: capture.binding.clone(),
        }
    }
    fn duplicate_captures(&mut self, frame_token: u32, captures: &CBoxedSlice<Capture>) -> Vec<Capture> {
        let dup_captures: Vec<Capture> = captures
            .iter()
            .map(|capture| self.duplicate_capture(frame_token, capture))
            .collect();
        dup_captures
    }


    fn record_node_mapping(&mut self, frame_token: u32, old: NodeRef, new: NodeRef) {
        let mut old_original = old;
        while let Some(node_ref_t) = self.new2old.nodes.get(&old_original) {
            old_original = node_ref_t.clone();
        }
        self.old2new.nodes.entry(old_original).or_default().insert(frame_token, new);
        self.new2old.nodes.entry(new).or_insert(old_original);
    }
    fn record_block_mapping(
        &mut self,
        frame_token: u32,
        old: &Pooled<BasicBlock>,
        new: &Pooled<BasicBlock>,
    ) {
        let mut old_original = old.as_ptr();
        while let Some(bb_t) = self.new2old.blocks.get(&old_original) {
            old_original = bb_t.clone();
        }
        self.old2new.blocks.entry(old_original).or_default().insert(frame_token, new.clone());
        self.new2old.blocks.entry(new.as_ptr()).or_insert(old_original);
    }
    fn record_callable_mapping(
        &mut self,
        frame_token: u32,
        old: &CArc<CallableModule>,
        new: &CArc<CallableModule>,
    ) {
        let mut old_original = old.as_ptr();
        while let Some(callable_t) = self.new2old.callables.get(&old_original) {
            old_original = callable_t.clone();
        }
        self.old2new.callables.entry(old_original).or_default().insert(frame_token, new.clone());
        self.new2old.callables.entry(new.as_ptr()).or_insert(old_original);
    }
}

pub struct SplitCoro;

impl Transform for SplitCoro {
    fn transform_callable(&self, callable: CallableModule) -> CallableModule {
        println!("SplitCoro::transform_module");
        let graph = CoroGraph::from(&callable.module);
        graph.dump();
        let frame_analyser = CoroFrameAnalysis::analyse(&graph, &callable);
        let coroutine_entry = SplitManager::split(callable, graph, frame_analyser);

        println!("{:-^40}", " After split ");
        let result = DisplayIR::new().display_ir_callable(&coroutine_entry);
        println!("{:-^40}\n{}", format!(" CoroScope {} ", 0), result);
        for (token, coro) in coroutine_entry
            .subroutine_ids
            .iter()
            .zip(coroutine_entry.subroutines.iter())
        {
            let result = DisplayIR::new().display_ir_callable(coro.as_ref());
            println!("{:-^40}\n{}", format!(" CoroScope {} ", token), result);
        }

        coroutine_entry
    }
}
