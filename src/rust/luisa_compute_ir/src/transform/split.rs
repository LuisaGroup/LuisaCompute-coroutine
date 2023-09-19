use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::ptr::null;
use lazy_static::lazy_static;

use crate::{CArc, CBoxedSlice, Pooled};
use crate::analysis::coro_frame::{CoroFrameAnalyser, VisitState};
use crate::analysis::frame_token_manager::FrameTokenManager;
use crate::ir::{SwitchCase, Instruction, BasicBlock, KernelModule, IrBuilder, ModulePools, NodeRef, Node, new_node, Type, Capture, INVALID_REF, CallableModule, Func, PhiIncoming, Module, CallableModuleRef, StructType};

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

#[derive(Debug, Default)]
struct CallableModuleInfo {
    args: Vec<NodeRef>,
    captures: Vec<Capture>,
}

#[derive(Debug)]
struct CoroFrame {
    token: u32,
    frame_node: NodeRef,
    frame_type: CArc<Type>,
}


pub(crate) struct SplitManager {
    frame_analyser: CoroFrameAnalyser,

    old2new: Old2NewMap,
    new2old: New2OldMap,

    // TODO: set private and add an API
    pub(crate) coro_scopes: HashMap<u32, Pooled<BasicBlock>>,
    coro_callable_info: HashMap<u32, CallableModuleInfo>,
}

impl SplitManager {
    pub(crate) fn split(frame_analyser: CoroFrameAnalyser, callable: &CallableModule) -> Self {
        let mut sm = Self {
            frame_analyser,
            old2new: Default::default(),
            new2old: Default::default(),
            coro_scopes: HashMap::new(),
            coro_callable_info: HashMap::new(),
        };

        // prepare
        let pools = &callable.pools;
        let scope_builder = sm.preprocess(callable);
        let bb = &callable.module.entry;

        // visit
        let mut sb_vec = sm.visit_bb(pools, VisitState::new_whole(bb), scope_builder);
        while !sb_vec.is_empty() {
            let scope_builder = sb_vec.remove(0);
            sm.build_scope(scope_builder, true);
        }
        sm
    }


    fn create_scope_builder_temp(&mut self, pools: CArc<ModulePools>) -> ScopeBuilder {
        ScopeBuilder::new(FrameTokenManager::get_temp_token(), pools)
    }

    // fn preprocess_bb(&mut self, bb: &Pooled<BasicBlock>, callable_original: &CallableModule) {
    //     bb.iter().for_each(|node_ref_present| {
    //         let node = node_ref_present.get();
    //         match node.instruction.as_ref() {
    //             Instruction::CoroSplitMark { token } => {
    //                 // TODO: args & captures
    //                 let args = self.duplicate_args(*token, &callable_original.pools, &callable_original.args);
    //                 let captures = self.duplicate_captures(*token, &callable_original.pools, &callable_original.captures);
    //             }
    //             // 3 Instructions after CCF
    //             Instruction::Loop { body, cond } => {
    //                 self.preprocess_bb(body, callable_original);
    //             }
    //             Instruction::If { cond, true_branch, false_branch } => {
    //                 self.preprocess_bb(true_branch, callable_original);
    //                 self.preprocess_bb(false_branch, callable_original);
    //             }
    //             Instruction::Switch {
    //                 value: _,
    //                 default,
    //                 cases,
    //             } => {
    //                 self.preprocess_bb(default, callable_original);
    //                 for SwitchCase { value: _, block } in cases.as_ref().iter() {
    //                     self.preprocess_bb(block, callable_original);
    //                 }
    //             }
    //             _ => {}
    //         }
    //     });
    // }
    fn preprocess(&mut self, callable: &CallableModule) -> ScopeBuilder {
        let pools = &callable.pools;
        let entry_token = self.frame_analyser.entry_token;

        // self.preprocess_bb(bb, callable);

        // duplicate frames as args
        let token_vec = self.frame_analyser.active_vars.keys().cloned().collect::<Vec<_>>();
        for token in token_vec.iter() {
            let mut args = vec![];
            let mut captures = vec![];
            let mut input_var = self.frame_analyser.active_vars.get(token).unwrap().input.clone();

            for arg in callable.args.as_ref() {
                if input_var.contains(arg) {
                    input_var.remove(arg);
                    let dup_arg = self.duplicate_arg(*token, pools, *arg);
                    args.push(dup_arg);
                }
            }
            for capture in callable.captures.as_ref() {
                if input_var.contains(&capture.node) {
                    input_var.remove(&capture.node);
                    let dup_capture = self.duplicate_capture(*token, pools, capture);
                    captures.push(dup_capture);
                }
            }

            // create coro frame
            let fields: Vec<_> = input_var.iter().map(|node_ref| {
                let node = node_ref.get();
                node.type_.clone()
            }).collect();
            let alignment = fields.iter().map(|type_| type_.alignment()).max().unwrap();
            let size = fields.iter().map(|type_| type_.size()).sum();
            let frame_type = crate::context::register_type(Type::Struct(StructType {
                fields: CBoxedSlice::new(fields),
                alignment,
                size,
            }));
            // let coro_frame = CoroFrame {
            //     token: *token,
            //     frame_node:
            //     frame_type,
            // };
            todo!();

            let callable_info = self.coro_callable_info.entry(*token).or_default();
            callable_info.args.extend(args.to_vec());
            callable_info.captures.extend(captures.to_vec());

            // duplicate IN[B] as args
        }

        ScopeBuilder::new(entry_token, pools.clone())
    }

    fn build_scope(&mut self, scope_builder: ScopeBuilder, force: bool) {
        assert!(scope_builder.finished || force);

        // build scope
        let frame_token = scope_builder.token;
        let scope_bb = scope_builder.builder.finish();
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

    fn visit_bb(&mut self, pools: &CArc<ModulePools>, visit_state: VisitState, mut scope_builder: ScopeBuilder) -> Vec<ScopeBuilder> {
        assert!(!scope_builder.finished);

        let mut node_ref_present = visit_state.start;
        while node_ref_present != visit_state.end {
            let node = node_ref_present.get();
            let type_ = &node.type_;
            let instruction = node.instruction.as_ref();
            println!("{:?}: {:?}", visit_state.present, instruction);

            match instruction {
                // coroutine related instructions
                Instruction::CoroSplitMark { token } => {
                    // TODO: different behaviors for loop/if
                    let (sb_before, sb_after) = self.visit_coro_split_mark(scope_builder, *token, node_ref_present.clone());
                    let visit_state_after = VisitState::new(visit_state.get_bb_ref(), Some(node_ref_present.get().next), None, None);
                    let mut sb_vec = self.visit_bb(pools, visit_state_after, sb_after);
                    // index 0 for the scope block before coro split mark
                    sb_vec.insert(0, sb_before);
                    return sb_vec;
                }
                Instruction::CoroSuspend { token } => {
                    let sb_before = self.visit_coro_suspend(scope_builder);
                    return vec![sb_before];
                }
                Instruction::CoroResume { token } => {
                    unreachable!("Split: CoroResume");
                }

                // 3 Instructions after CCF
                Instruction::Loop { body, cond } => {
                    let mut visit_state_after = visit_state.clone();
                    visit_state_after.present = node_ref_present;
                    let mut visit_result = self.visit_loop(pools, scope_builder, visit_state_after, body, cond);
                    if visit_result.split_possibly {
                        return visit_result.result;
                    } else {
                        assert_eq!(visit_result.result.len(), 1);
                        scope_builder = visit_result.result.pop().unwrap();
                    }
                }
                Instruction::If { cond, true_branch, false_branch } => {
                    let mut visit_state_after = visit_state.clone();
                    visit_state_after.present = node_ref_present;
                    let mut visit_result = self.visit_if(pools, scope_builder, visit_state_after, true_branch, false_branch, cond);
                    if visit_result.split_possibly {
                        return visit_result.result;
                    } else {
                        assert_eq!(visit_result.result.len(), 1);
                        scope_builder = visit_result.result.pop().unwrap();
                    }
                }
                Instruction::Switch { value, cases, default } => {
                    let mut visit_state_after = visit_state.clone();
                    visit_state_after.present = node_ref_present;
                    let mut visit_result = self.visit_switch(pools, scope_builder, visit_state_after, value, cases, default);
                    if visit_result.split_possibly {
                        return visit_result.result;
                    } else {
                        assert_eq!(visit_result.result.len(), 1);
                        scope_builder = visit_result.result.pop().unwrap();
                    }
                }

                // other instructions, just deep clone and change the node_ref to new ones
                _ => {
                    self.duplicate_node(&mut scope_builder, node_ref_present);
                }
            }
            node_ref_present = node.next;
        }
        vec![scope_builder]
    }
    fn visit_coro_split_mark(&mut self, mut sb_before: ScopeBuilder, token_next: u32, node_ref: NodeRef) -> (ScopeBuilder, ScopeBuilder) {
        let mut builder = &mut sb_before.builder;

        // replace CoroSplitMark with CoroSuspend
        let coro_suspend = builder.coro_suspend(token_next);
        node_ref.replace_with(coro_suspend.get());

        // create a new scope builder for the next scope
        // the next frame must have a CoroResume
        let mut sb_after = ScopeBuilder::new(token_next, builder.pools.clone());
        sb_after.builder.coro_resume(token_next);
        sb_before.finished = true;
        (sb_before, sb_after)
    }
    fn visit_coro_suspend(&mut self, mut scope_builder: ScopeBuilder) -> ScopeBuilder {
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

            // process true/false branch
            let mut all_branches_finished = true;
            let dup_true_branch = if split_poss_true.possibly {
                let sb_true = self.visit_branch_split(pools, scope_builder.token, true_branch, &mut sb_after_vec);
                all_branches_finished &= sb_true.finished;
                sb_true.builder.finish()
            } else {
                self.duplicate_block(scope_builder.token, &scope_builder.builder.pools, true_branch)
            };
            let dup_false_branch = if split_poss_false.possibly {
                let sb_false = self.visit_branch_split(pools, scope_builder.token, false_branch, &mut sb_after_vec);
                all_branches_finished &= sb_false.finished;
                sb_false.builder.finish()
            } else {
                self.duplicate_block(scope_builder.token, &scope_builder.builder.pools, false_branch)
            };
            let dup_cond = self.find_duplicated_node(scope_builder.token, *cond);
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

        let mut visit_result = VisitResult::new();

        if !split_poss_cases.possibly && !split_poss_default.possibly {
            // no split, duplicate the node
            self.duplicate_node(&mut scope_builder, visit_state.present);
            visit_result.split_possibly = false;
            visit_result.result.push(scope_builder);
        } else {
            // split in cases/default
            visit_result.split_possibly = true;
            let mut sb_after_vec = vec![];

            // process cases
            let mut all_branches_finished = true;
            let dup_cases: Vec<_> = cases_ref.iter().enumerate().map(|(i, case)| {
                let dup_block = if split_poss_case_vec[i].possibly {
                    let sb_case = self.visit_branch_split(pools, scope_builder.token, &case.block, &mut sb_after_vec);
                    all_branches_finished &= sb_case.finished;
                    sb_case.builder.finish()
                } else {
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
            let dup_value = self.find_duplicated_node(scope_builder.token, *value);
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
        let split_poss = self.frame_analyser.split_possibility.get(&body.as_ptr()).unwrap();
        assert_eq!(split_poss.possibly, true);

        if split_poss.definitely {
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
            // let sb_temp = self.create_scope_builder_temp(pools.clone());
            // let mut sb_vec = self.visit_bb(pools, VisitState::new_whole(body), sb_temp);
            //
            // let dup_cond = self.find_duplicated_node(scope_builder.token, *cond);
            // scope_builder.builder.loop_(bb_body_before_split, dup_cond);
        }
        todo!()
    }


    // duplicate functions
    fn find_duplicated_block(&mut self, frame_token: u32, bb: &Pooled<BasicBlock>) -> Pooled<BasicBlock> {
        self.old2new.blocks.get(&bb.as_ptr()).unwrap().get(&frame_token).unwrap().clone()
    }
    fn find_duplicated_node(&mut self, frame_token: u32, node: NodeRef) -> NodeRef {
        if !node.valid() { return INVALID_REF; }
        self.old2new.nodes.get(&node).unwrap().get(&frame_token).unwrap().clone()
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
        assert!(!self.old2new.blocks.get(&bb.as_ptr()).unwrap().contains_key(&frame_token),
                "Basic block {:?} has already been duplicated", bb);
        let mut scope_builder = ScopeBuilder::new(frame_token, pools.clone());
        bb.iter().for_each(|node| {
            self.duplicate_node(&mut scope_builder, node);
        });
        let dup_bb = scope_builder.builder.finish();
        // insert the duplicated block into the map
        self.record_block_mapping(frame_token, bb, &dup_bb);
        dup_bb
    }
    fn duplicate_module(&mut self, frame_token: u32, module: &Module) -> Module {
        let dup_entry = self.duplicate_block(frame_token, &module.pools, &module.entry);
        Module {
            kind: module.kind,
            entry: dup_entry,
            pools: module.pools.clone(),
        }
    }

    fn duplicate_node(&mut self, scope_builder: &mut ScopeBuilder, node_ref: NodeRef) -> NodeRef {
        if !node_ref.valid() { return INVALID_REF; }
        let frame_token = scope_builder.token;
        let mut builder = &mut scope_builder.builder;
        let node = node_ref.get();
        assert!(!self.old2new.nodes.entry(node_ref).or_default().contains_key(&frame_token),
                "Node {:?} has already been duplicated", node);
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
                let dup_init = self.find_duplicated_node(frame_token, *init);
                builder.local(dup_init)
            }
            Instruction::UserData(data) => builder.userdata(data.clone()),
            Instruction::Const(const_) => builder.const_(const_.clone()),
            Instruction::Update { var, value } => {
                // unreachable if SSA
                let dup_var = self.find_duplicated_node(frame_token, *var);
                let dup_value = self.find_duplicated_node(frame_token, *value);
                builder.update(dup_var, dup_value)
            }
            Instruction::Call(func, args) => {
                let dup_func = match func {
                    Func::Callable(callable) => {
                        let dup_callable = self.duplicate_callable(frame_token, &callable.0);
                        Func::Callable(CallableModuleRef(dup_callable))
                    }
                    _ => func.clone()
                };
                let dup_args: Vec<_> = args.iter().map(|arg| {
                    let dup_arg = self.find_duplicated_node(frame_token, *arg);
                    dup_arg
                }).collect();
                builder.call(dup_func, dup_args.as_slice(), node.type_.clone())
            }
            Instruction::Phi(incomings) => {
                let dup_incomings: Vec<_> = incomings.iter().map(|incoming| {
                    let dup_block = self.find_duplicated_block(frame_token, &incoming.block);
                    let dup_value = self.find_duplicated_node(frame_token, incoming.value);
                    PhiIncoming {
                        value: dup_value,
                        block: dup_block,
                    }
                }).collect();
                builder.phi(dup_incomings.as_slice(), node.type_.clone())
            }
            Instruction::Return(value) => {
                let dup_value = self.find_duplicated_node(frame_token, *value);
                builder.return_(dup_value)
            }
            Instruction::Loop { body, cond } => {
                let dup_body = self.duplicate_block(frame_token, &builder.pools, body);
                let dup_cond = self.find_duplicated_node(frame_token, *cond);
                builder.loop_(dup_body, dup_cond)
            }
            Instruction::GenericLoop { prepare, cond, body, update } => {
                let dup_prepare = self.duplicate_block(frame_token, &builder.pools, prepare);
                let dup_body = self.duplicate_block(frame_token, &builder.pools, body);
                let dup_update = self.duplicate_block(frame_token, &builder.pools, update);
                let dup_cond = self.find_duplicated_node(frame_token, *cond);
                builder.generic_loop(dup_prepare, dup_cond, dup_body, dup_update)
            }
            Instruction::Break => builder.break_(),
            Instruction::Continue => builder.continue_(),
            Instruction::If { cond, true_branch, false_branch } => {
                let dup_cond = self.find_duplicated_node(frame_token, *cond);
                let dup_true_branch = self.duplicate_block(frame_token, &builder.pools, true_branch);
                let dup_false_branch = self.duplicate_block(frame_token, &builder.pools, false_branch);
                builder.if_(dup_cond, dup_true_branch, dup_false_branch)
            }
            Instruction::Switch { value, cases, default } => {
                let dup_value = self.find_duplicated_node(frame_token, *value);
                let dup_cases: Vec<_> = cases.iter().map(|case| {
                    let dup_block = self.duplicate_block(frame_token, &builder.pools, &case.block);
                    SwitchCase {
                        value: case.value,
                        block: dup_block,
                    }
                }).collect();
                let dup_default = self.duplicate_block(frame_token, &builder.pools, default);
                builder.switch(dup_value, dup_cases.as_slice(), dup_default)
            }
            Instruction::AdScope { body } => {
                let dup_body = self.duplicate_block(frame_token, &builder.pools, body);
                builder.ad_scope(dup_body)
            }
            Instruction::RayQuery { ray_query, on_triangle_hit, on_procedural_hit } => {
                let dup_ray_query = self.find_duplicated_node(frame_token, *ray_query);
                let dup_on_triangle_hit = self.duplicate_block(frame_token, &builder.pools, on_triangle_hit);
                let dup_on_procedural_hit = self.duplicate_block(frame_token, &builder.pools, on_procedural_hit);
                builder.ray_query(dup_ray_query, dup_on_triangle_hit, dup_on_procedural_hit, node.type_.clone())
            }
            Instruction::AdDetach(body) => {
                let dup_body = self.duplicate_block(frame_token, &builder.pools, body);
                builder.ad_detach(dup_body)
            }
            Instruction::Comment(msg) => builder.comment(msg.clone()),
            Instruction::CoroSplitMark { token } => builder.coro_split_mark(*token),
            Instruction::CoroSuspend { token } => builder.coro_suspend(*token),
            Instruction::CoroResume { .. } => unreachable!("Unexpected instruction {:?} in SplitManager::duplicate_node", instruction),
        };
        // insert the duplicated node into the map
        self.record_node_mapping(frame_token, node_ref, dup_node);
        dup_node
    }
}