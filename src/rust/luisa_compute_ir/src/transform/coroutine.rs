use super::Transform;

use std::collections::{HashMap, HashSet};

use crate::{display::DisplayIR, CArc, CBoxedSlice, Pooled};
use crate::analysis::coro_frame_v3::{CoroFrameAnalyser, VisitState};
use crate::analysis::frame_token_manager::INVALID_FRAME_TOKEN_MASK;
use crate::context::register_type;
use crate::ir::{SwitchCase, Instruction, BasicBlock, IrBuilder, ModulePools, NodeRef, Node, new_node, Type, Capture, INVALID_REF, CallableModule, Func, PhiIncoming, Module, CallableModuleRef, StructType, Const, Primitive, VectorType, VectorElementType, ModuleFlags, ModuleKind};

const STATE_INDEX_CORO_ID: usize = 0;
const STATE_INDEX_FRAME_TOKEN: usize = 1;

struct ScopeBuilder {
    token: u32,
    finished: bool,
    builder: IrBuilder,
}

impl ScopeBuilder {
    fn new(frame_token: u32, pools: CArc<ModulePools>) -> Self {
        Self {
            token: frame_token,
            finished: false,
            builder: IrBuilder::new(pools),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct SplitPossibility {
    possibly: bool,
    directly: bool,
    definitely: bool,
}

struct VisitResult {
    split_possibly: bool,
    result: Vec<ScopeBuilder>,
}

impl VisitResult {
    fn new() -> Self {
        Self {
            split_possibly: true,
            result: vec![],
        }
    }
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

#[derive(Debug, Default, Clone)]
struct CallableModuleInfo {
    args: Vec<NodeRef>,
    captures: Vec<Capture>,
    frame_node: NodeRef,
    old2frame_index: HashMap<NodeRef, usize>,
    register_var2index: HashMap<u32, usize>,
}


pub(crate) struct SplitManager {
    frame_analyser: CoroFrameAnalyser,

    old2new: Old2NewMap,
    new2old: New2OldMap,

    coro_scopes: HashMap<u32, Pooled<BasicBlock>>,
    coro_local_builder: HashMap<u32, IrBuilder>,
    coro_callable_info: HashMap<u32, CallableModuleInfo>,
    frame_type: CArc<Type>,
    frame_fields: Vec<CArc<Type>>,

    display_ir: DisplayIR,  // for debug
}

impl SplitManager {
    pub(crate) fn split(frame_analyser: CoroFrameAnalyser, callable: &CallableModule) -> CallableModule {
        let mut sm = Self {
            frame_analyser,
            old2new: Default::default(),
            new2old: Default::default(),
            coro_scopes: HashMap::new(),
            coro_local_builder: HashMap::new(),
            coro_callable_info: HashMap::new(),
            frame_type: CArc::new(Type::Void),
            frame_fields: vec![],
            display_ir: DisplayIR::new(),
        };

        let _ = sm.display_ir.display_ir_callable(callable);    // for DEBUG

        // prepare
        let pools = &callable.pools;
        let bb = &callable.module.entry;
        let scope_builder = sm.pre_process(callable);
        // coroutine cannot return to entry scope, so do not resume here

        // visit
        let sb_vec = sm.visit_bb(pools, VisitState::new_whole(bb), scope_builder);
        assert!(sm.build_scopes(sb_vec, true).is_empty());
        sm.build_coroutines(callable)
    }

    fn build_coroutines(&self, callable: &CallableModule) -> CallableModule {
        let entry_token = self.frame_analyser.entry_token;
        let mut subroutines = vec![];
        let mut subroutine_ids = vec![];
        for (token, callable_info) in self.coro_callable_info.iter() {
            if *token == entry_token { continue; }
            let scope = self.coro_scopes.get(token).unwrap();
            let coroutine = CallableModule {
                module: Module {
                    kind: ModuleKind::Function,
                    entry: *scope,
                    flags: ModuleFlags::NONE,   // TODO: auto diff not allowed
                    pools: callable.pools.clone(),
                },
                ret_type: CArc::new(Type::Void),    // TODO: coroutine return type: only void allowed
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
                    pools: callable.pools.clone(),
                },
                ret_type: CArc::new(Type::Void),    // TODO: coroutine return type: only void allowed
                args: CBoxedSlice::from(callable_info.args.as_slice()),
                captures: CBoxedSlice::from(callable_info.captures.as_slice()),
                subroutines: CBoxedSlice::from(subroutines.as_slice()),
                subroutine_ids: CBoxedSlice::from(subroutine_ids.as_slice()),
                cpu_custom_ops: callable.cpu_custom_ops.clone(),
                pools: callable.pools.clone(),
            }
        }
    }

    fn pre_process(&mut self, callable: &CallableModule) -> ScopeBuilder {
        let pools = &callable.pools;
        let entry_token = self.frame_analyser.entry_token;

        let coro_id_type = Type::Vector(VectorType {
            element: VectorElementType::Scalar(Primitive::Uint32),
            length: 3,
        });
        let token_type = Type::Primitive(Primitive::Uint32);
        let mut frame_fields: Vec<CArc<Type>> = vec![CArc::new(coro_id_type), CArc::new(token_type)];
        let mut index_counter: usize = frame_fields.len();

        // calculate frame state
        let token_vec = self.frame_analyser.continuations.keys().cloned().collect::<Vec<_>>();
        let mut free_fields: HashMap<CArc<Type>, Vec<usize>> = HashMap::new();
        for token in token_vec.iter() {
            let args = self.duplicate_args(*token, pools, &callable.args);
            let captures = self.duplicate_captures(*token, pools, &callable.captures);
            let frame_vars = self.frame_analyser.frame_vars(*token);
            self.coro_local_builder.insert(*token, IrBuilder::new(pools.clone()));

            let callable_info = self.coro_callable_info.entry(*token).or_default();
            callable_info.args.extend(args.to_vec());
            callable_info.captures.extend(captures.to_vec());

            // create coro frame for Load
            // TODO: relocation temp vars
            let frame_vars: Vec<NodeRef> = frame_vars.iter().map(ToOwned::to_owned).collect();
            let fields: Vec<_> = frame_vars.iter().map(|node_ref| {
                let node = node_ref.get();
                node.type_.clone()
            }).collect();

            // TODO: frame slot strategies?

            // // 1. join all fields together
            // for i in 0..fields.len() {
            //     callable_info.old2frame_index.insert(frame_vars[i], index_counter + i);
            // }
            // index_counter += fields.len();
            // frame_fields.extend(fields);

            // 2. reuse fields of the same type
            let mut used: HashSet<usize> = HashSet::from([0, 1]);
            let mut new_fields = Vec::new();
            for i in 0..fields.len() {
                let field = &fields[i];
                let free_field = free_fields.entry(field.clone()).or_default();
                let index = free_field.iter().find(|index| !used.contains(index));
                let index = if let Some(index) = index {
                    *index
                } else {
                    let index = index_counter;
                    index_counter += 1;
                    free_field.push(index);
                    new_fields.push(field.clone());
                    index
                };
                used.insert(index);
                callable_info.old2frame_index.insert(frame_vars[i], index);
            }
            frame_fields.extend(new_fields);
        }

        let alignment = frame_fields.iter().map(|type_| type_.alignment()).max().unwrap();
        let size = frame_fields.iter().map(|type_| type_.size()).sum();
        self.frame_fields = frame_fields.clone();
        self.frame_type = register_type(Type::Struct(StructType {
            fields: CBoxedSlice::new(frame_fields),
            alignment,
            size,
        }));

        for token in token_vec.iter() {
            let callable_info = self.coro_callable_info.get_mut(token).unwrap();

            // args[0] is frame by default
            // change the type of frame here
            callable_info.args[0].get_mut().type_ = self.frame_type.clone();
            callable_info.frame_node = callable_info.args[0].clone();
        }

        ScopeBuilder::new(entry_token, pools.clone())
    }

    fn build_scope(&mut self, mut scope_builder: ScopeBuilder, force: bool) {
        if !(scope_builder.finished || force) {
            return;
        }

        // build scope
        let frame_token = scope_builder.token;
        if !scope_builder.finished && force {
            scope_builder.finished = true;
            self.coro_store(&mut scope_builder, INVALID_FRAME_TOKEN_MASK);
            scope_builder.builder.coro_suspend(INVALID_FRAME_TOKEN_MASK);
            scope_builder.builder.return_(INVALID_REF);
        }
        let scope_bb = scope_builder.builder.finish();
        let mut ir_builder = self.coro_local_builder.remove(&frame_token).unwrap();
        ir_builder.append_block(scope_bb);
        let scope_bb = ir_builder.finish();
        self.coro_scopes.insert(frame_token, scope_bb);
    }
    fn build_scopes(&mut self, mut sb_vec: Vec<ScopeBuilder>, force: bool) -> Vec<ScopeBuilder> {
        let mut sb_ans_vec = vec![];
        while !sb_vec.is_empty() {
            let scope_builder = sb_vec.remove(0);
            if scope_builder.finished || force {
                self.build_scope(scope_builder, force);
            } else {
                sb_ans_vec.push(scope_builder);
            }
        }
        sb_ans_vec
    }


    fn coro_resume(&mut self, mut scope_builder: ScopeBuilder) -> ScopeBuilder {
        let token = scope_builder.token;
        let builder = &mut scope_builder.builder;
        builder.coro_resume(token);   // TODO: delete
        builder.comment(CBoxedSlice::from("CoroResume Start".as_bytes()));   // TODO: for DEBUG
        let old2frame_index = self.coro_callable_info.get(&token).unwrap().old2frame_index.clone();
        let frame_node = self.coro_callable_info.get(&token).unwrap().frame_node;
        for (old_node, index) in old2frame_index.iter() {
            let index_node = builder.const_(Const::Uint32(*index as u32));
            let gep = builder.gep_chained(
                frame_node,
                &[index_node],
                self.frame_fields[*index].clone());
            self.record_node_mapping(token, *old_node, gep);
            // // TODO: debug
            // if token == 1 {
            //     println!("{} => {}", self.display_ir.var_str(old_node), self.display_ir.var_str_or_insert(&gep));
            // }
        }
        builder.comment(CBoxedSlice::from("CoroResume End".as_bytes()));   // TODO: for DEBUG
        scope_builder
    }
    fn coro_store(&mut self, scope_builder: &mut ScopeBuilder, token_next: u32) {
        let token = scope_builder.token;
        let builder = &mut scope_builder.builder;
        builder.comment(CBoxedSlice::from("CoroSuspend Start".as_bytes()));   // TODO: for DEBUG

        let frame_node = self.coro_callable_info.get(&token).unwrap().frame_node;
        if token_next & INVALID_FRAME_TOKEN_MASK == 0 {
            let old2frame_index = &self.coro_callable_info.get(&token_next).unwrap().old2frame_index;
            // store frame state
            for (old_node, index) in old2frame_index.iter() {
                let index_node = builder.const_(Const::Uint32(*index as u32));
                // TODO: debug
                if self.old2new.nodes.get(old_node) == None {
                    println!("token: {}, token_next: {:?}", token, token_next);
                    println!("old_node: {}", self.display_ir.var_str(old_node));
                    panic!()
                }
                let old2new_map = self.old2new.nodes.get(old_node).unwrap();
                // TODO: debug
                if old2new_map.get(&token) == None {
                    println!("token: {}, token_next: {:?}", token, token_next);
                    println!("old_node: {}", self.display_ir.var_str(old_node));
                    panic!()
                }
                let value = old2new_map.get(&token).unwrap().clone();
                let value = if value.is_lvalue() {
                    builder.load(value)
                } else {
                    value
                };
                let gep = builder.gep_chained(
                    frame_node,
                    &[index_node],
                    self.frame_fields[*index].clone());
                builder.update(gep, value);
            }
        };

        // change frame token
        let index_node = builder.const_(Const::Uint32(STATE_INDEX_FRAME_TOKEN as u32));
        let gep = builder.gep_chained(
            frame_node,
            &[index_node],
            self.frame_fields[STATE_INDEX_FRAME_TOKEN].clone());
        let value = builder.const_(Const::Uint32(token_next));
        builder.update(gep, value);
        builder.comment(CBoxedSlice::from("CoroSuspend End".as_bytes()));   // TODO: for DEBUG
    }

    fn visit_bb(&mut self, pools: &CArc<ModulePools>, visit_state: VisitState, mut scope_builder: ScopeBuilder) -> Vec<ScopeBuilder> {
        assert!(!scope_builder.finished);

        let mut visit_state = visit_state;
        while visit_state.present != visit_state.end {
            let mut node = visit_state.present.get();
            let type_ = &node.type_;
            let instruction = node.instruction.as_ref();
            // println!("Token {}, Visit noderef {:?} : {:?}", scope_builder.token, visit_state.present.0, instruction);

            match instruction {
                // coroutine related instructions
                Instruction::CoroSplitMark { token } => {
                    let (sb_before, sb_after) = self.visit_coro_split_mark(scope_builder, *token, visit_state.present.clone());
                    let visit_state_after = VisitState::new(visit_state.get_bb_ref(), Some(visit_state.present.get().next), None, None);
                    let mut sb_vec = self.visit_bb(pools, visit_state_after, sb_after);
                    // index 0 for the scope block before coro split mark
                    sb_vec.insert(0, sb_before);
                    return sb_vec;
                }
                Instruction::CoroSuspend { token } => {
                    let sb_before = self.visit_coro_suspend(scope_builder, *token);
                    return vec![sb_before];
                }
                Instruction::CoroResume { token } => {
                    unreachable!("Split: CoroResume");
                }
                Instruction::CoroRegister { token, value, var } => {
                    // var <-> frame[token][index] <-> value
                    assert_eq!(*token, scope_builder.token);
                    let callable_info = self.coro_callable_info.get_mut(token).unwrap();
                    let index = callable_info.old2frame_index.get(value).unwrap();
                    callable_info.register_var2index.insert(*var, *index);
                }

                // 3 Instructions after CCF
                Instruction::Loop { body, cond } => {
                    let mut visit_result = self.visit_loop(pools, scope_builder, visit_state.clone(), body, cond);
                    if visit_result.split_possibly {
                        return visit_result.result;
                    } else {
                        assert_eq!(visit_result.result.len(), 1);
                        scope_builder = visit_result.result.pop().unwrap();
                    }
                }
                Instruction::If { cond, true_branch, false_branch } => {
                    let mut visit_result = self.visit_if(pools, scope_builder, visit_state.clone(), true_branch, false_branch, cond);
                    if visit_result.split_possibly {
                        return visit_result.result;
                    } else {
                        assert_eq!(visit_result.result.len(), 1);
                        scope_builder = visit_result.result.pop().unwrap();
                    }
                }
                Instruction::Switch { value, cases, default } => {
                    let mut visit_result = self.visit_switch(pools, scope_builder, visit_state.clone(), value, cases, default);
                    if visit_result.split_possibly {
                        return visit_result.result;
                    } else {
                        assert_eq!(visit_result.result.len(), 1);
                        scope_builder = visit_result.result.pop().unwrap();
                    }
                }

                Instruction::Return(..) => {}

                // other instructions, just deep clone and change the node_ref to new ones
                _ => {
                    self.duplicate_node(&mut scope_builder, visit_state.present);
                }
            }
            visit_state.present = node.next;
        }
        vec![scope_builder]
    }
    fn visit_coro_split_mark(&mut self, mut sb_before: ScopeBuilder, token_next: u32, node_ref: NodeRef) -> (ScopeBuilder, ScopeBuilder) {
        // replace CoroSplitMark with CoroSuspend
        self.coro_store(&mut sb_before, token_next);
        let coro_suspend = sb_before.builder.coro_suspend(token_next);
        node_ref.replace_with(coro_suspend.get());

        // create a void type node for termination
        // TODO: now use Instruction::Return for termination
        sb_before.builder.return_(INVALID_REF);

        // create a new scope builder for the next scope
        // the next frame must have a CoroResume
        let mut sb_after = ScopeBuilder::new(token_next, sb_before.builder.pools.clone());
        sb_after = self.coro_resume(sb_after);
        sb_before.finished = true;
        (sb_before, sb_after)
    }
    fn visit_coro_suspend(&mut self, mut scope_builder: ScopeBuilder, token_next: u32) -> ScopeBuilder {
        // create a void type node for termination
        self.coro_store(&mut scope_builder, token_next);
        scope_builder.builder.coro_suspend(token_next);
        // TODO: now use Instruction::Return for termination
        scope_builder.builder.return_(INVALID_REF);

        scope_builder.finished = true;
        scope_builder
    }
    fn visit_branch_split(&mut self, pools: &CArc<ModulePools>, frame_token: u32,
                          branch: &Pooled<BasicBlock>, sb_after_vec: &mut Vec<ScopeBuilder>) -> ScopeBuilder {
        let scope_builder = ScopeBuilder::new(frame_token, pools.clone());
        let mut sb_vec = self.visit_bb(pools, VisitState::new_whole(branch), scope_builder);
        let sb_before_split = sb_vec.remove(0);
        sb_after_vec.extend(sb_vec);
        sb_before_split
    }
    fn visit_if(&mut self, pools: &CArc<ModulePools>, mut scope_builder: ScopeBuilder,
                visit_state: VisitState, true_branch: &Pooled<BasicBlock>, false_branch: &Pooled<BasicBlock>, cond: &NodeRef) -> VisitResult {
        /*

        BLOCK A;
        if (cond_1) {
            BLOCK B;
            split(1);
            BLOCK C;
        } else {
            BLOCK D;
        }
        BLOCK E;

        ----------------------
        | BLOCK A;           |
        | if (cond_1) {      |
        |     BLOCK B;       |
        |     suspend(1);    |----
        | } else {           |   |
        |     BLOCK D;       |   |
        | }                  |   |
        | BLOCK E;           |   |
        ----------------------   |
                                 |
                   ---------------
                   |
                   V
        ----------------------
        | resume(1);         |
        | BLOCK C;           |
        | BLOCK E;           |
        ______________________


        BLOCK A;
        if (cond_1) {
            BLOCK B;
            split(1);
            BLOCK C;
        } else {
            BLOCK D;
            split(2);
            BLOCK E;
        }
        BLOCK F;
        if (cond_2) {
            BLOCK G;
            split(3);
            BLOCK H;
        } else {
            BLOCK I;
            split(4);
            BLOCK J;
        }
        BLOCK K;

        ----------------------
        | BLOCK A;           |
        | if (cond_1) {      |
        |     BLOCK B;       |
        |     suspend(1);    |----
        | } else {           |   |
        |     BLOCK D;       |   |
        |     suspend(2);    |---|-----------
        | }                  |   |          |
        ----------------------   |          |
                                 |          |
                   ---------------          |
                   |                        |
                   V                        V
        ----------------------    ----------------------
        | resume(1);         |    | resume(2);         |
        | BLOCK C;           |    | BLOCK E;           |
        | BLOCK F;           |    | BLOCK F;           |
        | if (cond_2) {      |    | if (cond_2) {      |
        |     BLOCK G;       |    |     BLOCK G;       |
        |     suspend(3);    |----|     suspend(3);    |----
        | } else {           |    | } else {           |   |
        |     BLOCK I;       |    |     BLOCK I;       |   |
        |     suspend(4);    |----|     suspend(4);    |---|-----------
        | }                  |    | }                  |   |          |
        ----------------------    ----------------------   |          |
                                                           |          |
                   -----------------------------------------          |
                   |                        ---------------------------
                   |                        |
                   V                        V
        ----------------------    ----------------------
        | resume(3);         |    | resume(4);         |
        | BLOCK H;           |    | BLOCK J;           |
        | BLOCK K;           |    | BLOCK K;           |
        ______________________    ______________________

         */
        let mut visit_result = VisitResult::new();

        let split_poss_true = self.frame_analyser.split_possibility.get(&true_branch.as_ptr()).unwrap().clone();
        let split_poss_false = self.frame_analyser.split_possibility.get(&false_branch.as_ptr()).unwrap().clone();

        if !split_poss_true.possibly && !split_poss_false.possibly {
            // no split, duplicate the node
            self.duplicate_node(&mut scope_builder, visit_state.present);
            visit_result.split_possibly = false;
            visit_result.result.push(scope_builder);
        } else {
            // split in true/false_branch
            visit_result.split_possibly = true;
            let mut sb_after_vec = vec![];
            let dup_cond = self.find_duplicated_node(&mut scope_builder, *cond);

            // process true/false branch
            let mut all_branches_finished = true;
            let dup_true_branch = if split_poss_true.possibly {
                let sb_true = self.visit_branch_split(pools, scope_builder.token, true_branch, &mut sb_after_vec);
                all_branches_finished &= sb_true.finished;
                sb_true.builder.finish()
            } else {
                all_branches_finished = false;
                self.duplicate_block(scope_builder.token, &scope_builder.builder.pools, true_branch)
            };
            let dup_false_branch = if split_poss_false.possibly {
                let sb_false = self.visit_branch_split(pools, scope_builder.token, false_branch, &mut sb_after_vec);
                all_branches_finished &= sb_false.finished;
                sb_false.builder.finish()
            } else {
                all_branches_finished = false;
                self.duplicate_block(scope_builder.token, &scope_builder.builder.pools, false_branch)
            };
            scope_builder.builder.if_(dup_cond, dup_true_branch, dup_false_branch);
            scope_builder.finished |= all_branches_finished;

            // process next bb
            sb_after_vec.insert(0, scope_builder);

            let mut visit_state_after = visit_state.clone();
            visit_state_after.present = visit_state.present.get().next;
            for sb_after in sb_after_vec {
                if sb_after.finished {
                    visit_result.result.push(sb_after);
                } else {
                    visit_result.result.extend(self.visit_bb(pools, visit_state_after.clone(), sb_after));
                }
            }
        }
        visit_result
    }
    fn visit_switch(&mut self, pools: &CArc<ModulePools>, mut scope_builder: ScopeBuilder,
                    visit_state: VisitState, value: &NodeRef, cases: &CBoxedSlice<SwitchCase>, default: &Pooled<BasicBlock>) -> VisitResult {
        let mut visit_result = VisitResult::new();

        // split in cases/default
        let cases_ref = cases.as_ref();
        let mut split_poss_case_vec = Vec::with_capacity(cases_ref.len());
        let mut split_poss_cases = SplitPossibility {
            possibly: false,
            definitely: true,
            directly: false,
        };
        let split_poss_default = self.frame_analyser.split_possibility.get(&default.as_ptr()).unwrap().clone();
        for case in cases_ref.iter() {
            let split_poss_case = self.frame_analyser.split_possibility.get(&case.block.as_ptr()).unwrap();
            split_poss_case_vec.push(split_poss_case.clone());
            split_poss_cases.possibly |= split_poss_case.possibly;
            split_poss_cases.definitely &= split_poss_case.definitely;
        }

        if !split_poss_cases.possibly && !split_poss_default.possibly {
            // no split, duplicate the node
            self.duplicate_node(&mut scope_builder, visit_state.present);
            visit_result.split_possibly = false;
            visit_result.result.push(scope_builder);
        } else {
            // split in cases/default
            visit_result.split_possibly = true;
            let mut sb_after_vec = vec![];
            let dup_value = self.find_duplicated_node(&mut scope_builder, *value);

            // process cases
            let mut all_branches_finished = true;
            let dup_cases: Vec<_> = cases_ref.iter().enumerate().map(|(i, case)| {
                let dup_block = if split_poss_case_vec[i].possibly {
                    let sb_case = self.visit_branch_split(pools, scope_builder.token, &case.block, &mut sb_after_vec);
                    all_branches_finished &= sb_case.finished;
                    sb_case.builder.finish()
                } else {
                    all_branches_finished = false;
                    self.duplicate_block(scope_builder.token, &scope_builder.builder.pools, &case.block)
                };
                SwitchCase {
                    value: case.value,
                    block: dup_block,
                }
            }).collect();
            // process default
            let dup_default = if split_poss_default.possibly {
                let sb_default = self.visit_branch_split(pools, scope_builder.token, default, &mut sb_after_vec);
                all_branches_finished &= sb_default.finished;
                sb_default.builder.finish()
            } else {
                self.duplicate_block(scope_builder.token, &scope_builder.builder.pools, default)
            };
            scope_builder.builder.switch(dup_value, dup_cases.as_slice(), dup_default);
            scope_builder.finished |= all_branches_finished;

            // process next bb
            sb_after_vec.insert(0, scope_builder);

            let mut visit_state_after = visit_state.clone();
            visit_state_after.present = visit_state.present.get().next;
            for sb_after in sb_after_vec {
                if sb_after.finished {
                    visit_result.result.push(sb_after);
                } else {
                    visit_result.result.extend(self.visit_bb(pools, visit_state_after.clone(), sb_after));
                }
            }
        }
        visit_result
    }
    fn visit_loop(&mut self, pools: &CArc<ModulePools>, mut scope_builder: ScopeBuilder,
                  visit_state: VisitState, body: &Pooled<BasicBlock>, cond: &NodeRef) -> VisitResult {
        let mut visit_result = VisitResult::new();

        let split_poss = self.frame_analyser.split_possibility.get(&body.as_ptr()).unwrap();

        if split_poss.possibly {
            visit_result.split_possibly = true;
            // if split_poss.definitely {
            if false {
                /*
                BLOCK A;
                loop {
                    BLOCK B;
                    if (cond_0) {
                        BLOCK E;
                        split(1);
                        BLOCK F;
                    } else {
                        BLOCK G;
                        split(2);
                        BLOCK H;
                    }
                    BLOCK C;
                } cond(cond_1);
                BLOCK D;


                ----------------------
                | BLOCK A;           |
                | BLOCK B;           |
                | if (cond_0) {      |
                |     BLOCK E;       |
                |     suspend(1);    |----
                | } else {           |   |
                |     BLOCK G;       |   |
                |     suspend(2);    |---|------------------------------
                | }                  |   |                             |
                ----------------------   |                             |
                                         |                             |
                          ----------------                             |
                          |                                            |
                          |                          ----------------->|<--------------------
                          |                          |                 |                    |
                          |<-------------------- <---------------------|-----------------   |
                          |                    |     |                 |                |   |
                          V                    |     |                 V                |   |
                --------------------------     |     |      --------------------------  |   |
                | resume(1);             |     |     |      | resume(2);             |  |   |
                | BLOCK F;               |     |     |      | BLOCK H;               |  |   |
                | BLOCK C;               |     |     |      | BLOCK C;               |  |   |
                | if (cond_1) {          |     |     |      | if (cond_1) {          |  |   |
                |     loop {             |     |     |      |     loop {             |  |   |
                |         BLOCK B;       |     |     |      |         BLOCK B;       |  |   |
                |         if (cond_0) {  |     |     |      |         if (cond_0) {  |  |   |
                |             BLOCK E;   |     |     |      |             BLOCK E;   |  |   |
                |             suspend(1);|------     |      |             suspend(1);|---   |
                |         } else {       |           |      |         } else {       |      |
                |             BLOCK G;   |           |      |             BLOCK G;   |      |
                |             suspend(2);|------------      |             suspend(2);|-------
                |         }              |                  |         }              |
                |     } cond(cond_1)     |                  |     } cond(cond_1)     |
                | }                      |                  | }                      |
                | BLOCK D;               |                  | BLOCK D;               |
                --------------------------                  --------------------------
                 */
            } else {
                /*
                BLOCK A;
                loop {
                    BLOCK B;
                    if (cond_0) {
                        BLOCK E;
                        split(1);
                        BLOCK F;
                    }
                    BLOCK C;
                } cond(cond_1);
                BLOCK D;

                ----------------------
                | BLOCK A;           |
                | loop {             |
                |     BLOCK B;       |
                |     if (cond_0) {  |
                |         BLOCK E;   |
                |         suspend(1);|
                |     }              |
                |     BLOCK C;       |
                | } cond(cond_1)     |
                | BLOCK D;           |
                ----------------------
                          |
                          |<--------------------
                          |                    |
                          V                    |
                --------------------------     |
                | resume(1);             |     |
                | BLOCK F;               |     |
                | BLOCK C;               |     |
                | if (cond_1) {          |     |
                |     loop {             |     |
                |         BLOCK B;       |     |
                |         if (cond_0) {  |     |
                |             BLOCK E;   |     |
                |             suspend(1);|------
                |         }              |
                |         BLOCK C;       |
                |     } cond(cond_1)     |
                | }                      |
                | BLOCK D;               |
                --------------------------
                 */

                let mut sb_after_vec = vec![];
                let sb_before = self.visit_branch_split(pools, scope_builder.token, body, &mut sb_after_vec);
                let body_before_split = sb_before.builder.finish();
                // cond may be in the body block
                let dup_cond = self.find_duplicated_node(&mut scope_builder, *cond);
                scope_builder.builder.loop_(body_before_split, dup_cond);
                scope_builder.finished |= sb_before.finished;

                let mut visit_state_after = visit_state.clone();
                visit_state_after.present = visit_state.present.get().next;

                // process next bb
                if scope_builder.finished {
                    visit_result.result.push(scope_builder);
                } else {
                    visit_result.result.extend(self.visit_bb(pools, visit_state_after.clone(), scope_builder));
                }

                for mut sb_after in sb_after_vec {
                    if sb_after.finished {
                        visit_result.result.push(sb_after);
                    } else {
                        // FIXME: dup_cond multiple mapping may cause problems!
                        let dup_cond_if = self.find_duplicated_node(&mut sb_after, *cond);
                        let dup_body_loop = self.duplicate_block(sb_after.token, &pools, body);
                        let dup_cond_loop = self.find_duplicated_node(&mut sb_after, *cond);

                        let mut loop_builder = ScopeBuilder::new(sb_after.token, pools.clone());
                        loop_builder.builder.loop_(dup_body_loop, dup_cond_loop);
                        let loop_ = loop_builder.builder.finish();
                        let empty_bb = IrBuilder::new(pools.clone()).finish();
                        sb_after.builder.if_(dup_cond_if, loop_, empty_bb);
                        visit_result.result.extend(self.visit_bb(pools, visit_state_after.clone(), sb_after));
                    }
                }
            }
        } else {
            // no split, duplicate the node
            self.duplicate_node(&mut scope_builder, visit_state.present);
            visit_result.split_possibly = false;
            visit_result.result.push(scope_builder);
        }
        visit_result
    }


    // duplicate functions
    fn find_duplicated_block(&mut self, frame_token: u32, bb: &Pooled<BasicBlock>) -> Pooled<BasicBlock> {
        self.old2new.blocks.get(&bb.as_ptr()).unwrap().get(&frame_token).unwrap().clone()
    }
    fn find_duplicated_node(&mut self, scope_builder: &mut ScopeBuilder, node: NodeRef) -> NodeRef {
        if !node.valid() { return INVALID_REF; }
        let frame_token = scope_builder.token;
        if self.old2new.nodes.get(&node).unwrap().contains_key(&frame_token) {
            self.old2new.nodes.get(&node).unwrap().get(&frame_token).unwrap().clone()
        } else {
            unimplemented!("local_zero_init");
            // FIXME: this is a temporary solution, local_zero_init
            let type_ = node.type_().clone();
            let local = self.coro_local_builder.get_mut(&frame_token).unwrap().local_zero_init(type_);
            self.record_node_mapping(frame_token, node, local);
            self.old2new.nodes.get_mut(&node).unwrap().insert(frame_token, local.clone());
            local
        }
    }
    fn record_node_mapping(&mut self, frame_token: u32, old: NodeRef, new: NodeRef) {
        let mut old_original = old;
        while let Some(node_ref_t) = self.new2old.nodes.get(&old_original) {
            old_original = node_ref_t.clone();
        }
        self.old2new.nodes.entry(old_original).or_default().insert(frame_token, new);
        self.new2old.nodes.entry(new).or_insert(old_original);
    }
    fn record_block_mapping(&mut self, frame_token: u32, old: &Pooled<BasicBlock>, new: &Pooled<BasicBlock>) {
        let mut old_original = old.as_ptr();
        while let Some(bb_t) = self.new2old.blocks.get(&old_original) {
            old_original = bb_t.clone();
        }
        self.old2new.blocks.entry(old_original).or_default().insert(frame_token, new.clone());
        self.new2old.blocks.entry(new.as_ptr()).or_insert(old_original);
    }
    fn record_callable_mapping(&mut self, frame_token: u32, old: &CArc<CallableModule>, new: &CArc<CallableModule>) {
        let mut old_original = old.as_ptr();
        while let Some(callable_t) = self.new2old.callables.get(&old_original) {
            old_original = callable_t.clone();
        }
        self.old2new.callables.entry(old_original).or_default().insert(frame_token, new.clone());
        self.new2old.callables.entry(new.as_ptr()).or_insert(old_original);
    }
    fn duplicate_arg(&mut self, frame_token: u32, pools: &CArc<ModulePools>, node_ref: NodeRef) -> NodeRef {
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
            _ => unreachable!("Invalid argument type")
        };
        let dup_node = Node::new(dup_instr, node.type_.clone());
        let dup_node_ref = new_node(pools, dup_node);

        // add to node map
        self.record_node_mapping(frame_token, node_ref, dup_node_ref);
        dup_node_ref
    }
    fn duplicate_capture(&mut self, frame_token: u32, pools: &CArc<ModulePools>,
                         capture: &Capture) -> Capture {
        Capture {
            node: self.duplicate_arg(frame_token, pools, capture.node.clone()),
            binding: capture.binding.clone(),
        }
    }
    fn duplicate_args(&mut self, frame_token: u32, pools: &CArc<ModulePools>,
                      args: &CBoxedSlice<NodeRef>) -> CBoxedSlice<NodeRef> {
        let dup_args: Vec<NodeRef> = args.iter().map(|arg| {
            self.duplicate_arg(frame_token, pools, arg.clone())
        }).collect();
        CBoxedSlice::new(dup_args)
    }
    fn duplicate_captures(&mut self, frame_token: u32, pools: &CArc<ModulePools>,
                          captures: &CBoxedSlice<Capture>) -> CBoxedSlice<Capture> {
        let dup_captures: Vec<Capture> = captures.iter().map(|capture| {
            self.duplicate_capture(frame_token, pools, capture)
        }).collect();
        CBoxedSlice::new(dup_captures)
    }
    fn duplicate_shared(&mut self, frame_token: u32, pools: &CArc<ModulePools>,
                        shared: &CBoxedSlice<NodeRef>) -> CBoxedSlice<NodeRef> {
        let dup_shared: Vec<NodeRef> = shared.iter().map(|shared| {
            self.duplicate_arg(frame_token, pools, shared.clone())
        }).collect();
        CBoxedSlice::new(dup_shared)
    }

    fn duplicate_callable(&mut self, frame_token: u32, callable: &CArc<CallableModule>) -> CArc<CallableModule> {
        let pools = &callable.pools;
        if let Some(copy) = self.old2new.callables.get(&callable.as_ptr()).unwrap().get(&frame_token) {
            return copy.clone();
        }
        let dup_callable = {
            let dup_args = self.duplicate_args(frame_token, pools, &callable.args);
            let dup_captures = self.duplicate_captures(frame_token, pools, &callable.captures);
            let dup_module = self.duplicate_module(frame_token, &callable.module);
            CallableModule {
                module: dup_module,
                ret_type: callable.ret_type.clone(),
                args: dup_args,
                captures: dup_captures,
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
    fn duplicate_block(&mut self, frame_token: u32, pools: &CArc<ModulePools>, bb: &Pooled<BasicBlock>) -> Pooled<BasicBlock> {
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
        }
    }

    fn duplicate_node(&mut self, mut scope_builder: &mut ScopeBuilder, node_ref: NodeRef) -> NodeRef {
        if !node_ref.valid() { return INVALID_REF; }
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
            | Instruction::Argument { .. } => unreachable!("{:?} should not appear in basic block", instruction),
            Instruction::Invalid => unreachable!("Invalid node should not appear in non-sentinel nodes"),

            Instruction::Local { init } => {
                let dup_init = self.find_duplicated_node(scope_builder, *init);
                scope_builder.builder.local(dup_init)
            }
            Instruction::UserData(data) => scope_builder.builder.userdata(data.clone()),
            Instruction::Const(const_) => scope_builder.builder.const_(const_.clone()),
            Instruction::Update { var, value } => {
                // unreachable if SSA
                let dup_var = self.find_duplicated_node(scope_builder, *var);
                let dup_value = self.find_duplicated_node(scope_builder, *value);
                scope_builder.builder.update(dup_var, dup_value)
            }
            Instruction::Call(func, args) => {
                let dup_func = match func {
                    Func::Callable(callable) => {
                        let dup_callable = self.duplicate_callable(frame_token, &callable.0);
                        Func::Callable(CallableModuleRef(dup_callable))
                    }
                    _ => func.clone()
                };
                let mut dup_args: Vec<_> = args.iter().map(|arg| {
                    let dup_arg = self.find_duplicated_node(scope_builder, *arg);
                    dup_arg
                }).collect();
                if func == &Func::CoroId || func == &Func::CoroToken {
                    assert!(dup_args.is_empty(), "Args of function {:?} is not empty", func);
                    let frame_node = self.coro_callable_info.get(&frame_token).unwrap().frame_node;
                    dup_args.push(frame_node);
                }
                scope_builder.builder.call(dup_func, dup_args.as_slice(), node.type_.clone())
            }
            Instruction::Phi(incomings) => {
                let dup_incomings: Vec<_> = incomings.iter().map(|incoming| {
                    let dup_block = self.find_duplicated_block(frame_token, &incoming.block);
                    let dup_value = self.find_duplicated_node(scope_builder, incoming.value);
                    PhiIncoming {
                        value: dup_value,
                        block: dup_block,
                    }
                }).collect();
                scope_builder.builder.phi(dup_incomings.as_slice(), node.type_.clone())
            }
            Instruction::AdScope { body, .. } => {
                let dup_body = self.duplicate_block(frame_token, &scope_builder.builder.pools, body);
                scope_builder.builder.ad_scope(dup_body)
            }
            Instruction::RayQuery { ray_query, on_triangle_hit, on_procedural_hit } => {
                let dup_ray_query = self.find_duplicated_node(scope_builder, *ray_query);
                let dup_on_triangle_hit = self.duplicate_block(frame_token, &scope_builder.builder.pools, on_triangle_hit);
                let dup_on_procedural_hit = self.duplicate_block(frame_token, &scope_builder.builder.pools, on_procedural_hit);
                scope_builder.builder.ray_query(dup_ray_query, dup_on_triangle_hit, dup_on_procedural_hit, node.type_.clone())
            }
            Instruction::AdDetach(body) => {
                let dup_body = self.duplicate_block(frame_token, &scope_builder.builder.pools, body);
                scope_builder.builder.ad_detach(dup_body)
            }
            Instruction::Comment(msg) => scope_builder.builder.comment(msg.clone()),

            Instruction::Return(value) => {
                let dup_value = self.find_duplicated_node(scope_builder, *value);
                scope_builder.builder.return_(dup_value)
            }

            // Instruction::GenericLoop { prepare, cond, body, update } => {
            //     let dup_prepare = self.duplicate_block(frame_token, &scope_builder.builder.pools, prepare);
            //     let dup_body = self.duplicate_block(frame_token, &scope_builder.builder.pools, body);
            //     let dup_update = self.duplicate_block(frame_token, &scope_builder.builder.pools, update);
            //     let dup_cond = self.find_duplicated_node(scope_builder, *cond);
            //     scope_builder.builder.generic_loop(dup_prepare, dup_cond, dup_body, dup_update)
            // }
            // Instruction::Break => scope_builder.builder.break_(),
            // Instruction::Continue => scope_builder.builder.continue_(),

            Instruction::GenericLoop { .. }
            | Instruction::Break
            | Instruction::Continue => unreachable!("{:?} should be handled in CCF", instruction),

            Instruction::Loop { body, cond } => {
                let dup_body = self.duplicate_block(frame_token, &scope_builder.builder.pools, body);
                let dup_cond = self.find_duplicated_node(scope_builder, *cond);
                scope_builder.builder.loop_(dup_body, dup_cond)
            }
            Instruction::If { cond, true_branch, false_branch } => {
                let dup_cond = self.find_duplicated_node(scope_builder, *cond);
                let dup_true_branch = self.duplicate_block(frame_token, &scope_builder.builder.pools, true_branch);
                let dup_false_branch = self.duplicate_block(frame_token, &scope_builder.builder.pools, false_branch);
                scope_builder.builder.if_(dup_cond, dup_true_branch, dup_false_branch)
            }
            Instruction::Switch { value, cases, default } => {
                let dup_value = self.find_duplicated_node(scope_builder, *value);
                let dup_cases: Vec<_> = cases.iter().map(|case| {
                    let dup_block = self.duplicate_block(frame_token, &scope_builder.builder.pools, &case.block);
                    SwitchCase {
                        value: case.value,
                        block: dup_block,
                    }
                }).collect();
                let dup_default = self.duplicate_block(frame_token, &scope_builder.builder.pools, default);
                scope_builder.builder.switch(dup_value, dup_cases.as_slice(), dup_default)
            }

            Instruction::Print { fmt, args } => {
                let args = args
                    .iter()
                    .map(|x| self.find_duplicated_node(scope_builder, *x))
                    .collect::<Vec<_>>();
                scope_builder.builder.print(fmt.clone(), &args)
            }

            // Coro
            Instruction::CoroSuspend { token: token_next } => {
                self.coro_store(&mut scope_builder, *token_next);
                let node_new = scope_builder.builder.coro_suspend(*token_next);

                // create a void type node for termination
                // TODO: now use Instruction::Return for termination
                scope_builder.builder.return_(INVALID_REF);

                node_new
            }
            Instruction::CoroRegister { .. }
            | Instruction::CoroSplitMark { .. }
            | Instruction::CoroResume { .. } => unreachable!("Unexpected instruction {:?} in SplitManager::duplicate_node", instruction),
        };
        // insert the duplicated node into the map
        self.record_node_mapping(frame_token, node_ref, dup_node);
        dup_node
    }
}


pub struct CoroutineSplit;

struct CoroutineSplitImpl {}

impl CoroutineSplitImpl {
    fn split_coroutine(callable: CallableModule) -> CallableModule {
        println!("{:-^40}", " Before split ");
        let mut display_ir = DisplayIR::new();
        let result = display_ir.display_ir_callable(&callable);
        println!("{}", result);

        let mut coro_frame_analyser = CoroFrameAnalyser::new();
        coro_frame_analyser.analyse_callable(&callable);
        println!("{}", coro_frame_analyser.display_continuations());

        let coroutine_entry = SplitManager::split(coro_frame_analyser, &callable);
        println!("{:-^40}", " After split ");
        let result = DisplayIR::new().display_ir_callable(&coroutine_entry);
        println!("{:-^40}\n{}", format!(" CoroScope {} ", 0), result);
        for (token, coro) in coroutine_entry.subroutine_ids.iter().zip(coroutine_entry.subroutines.iter()) {
            let result = DisplayIR::new().display_ir_callable(coro.as_ref());
            println!("{:-^40}\n{}", format!(" CoroScope {} ", token), result);
        }

        coroutine_entry
        // unimplemented!("Coroutine split");
    }
}

impl Transform for CoroutineSplit {
    fn transform_callable(&self, callable: CallableModule) -> CallableModule {
        CoroutineSplitImpl::split_coroutine(callable)
    }
}
