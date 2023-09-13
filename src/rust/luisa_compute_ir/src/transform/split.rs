use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use lazy_static::lazy_static;

use crate::{CArc, CBoxedSlice, Pooled};
use crate::ir::{SwitchCase, Instruction, BasicBlock, KernelModule, IrBuilder, ModulePools, NodeRef, Node, new_node, Type, Capture, INVALID_REF, CallableModule, Func, PhiIncoming, Module, CallableModuleRef};

struct FrameTokenManager {
    frame_token_counter: u32,
    frame_token_occupied: HashSet<u32>,
    frame_token_temp: u32,
}

static INVALID_FRAME_TOKEN_MASK: u32 = 0x8000_0000;

impl FrameTokenManager {
    fn register_frame_token(token: u32) {
        let ftm = Self::get_instance();
        assert!(token < INVALID_FRAME_TOKEN_MASK, "Invalid frame token");
        ftm.frame_token_occupied.insert(token);
    }
    fn get_new_token() -> u32 {
        let ftm = Self::get_instance();
        while ftm.frame_token_occupied.contains(&ftm.frame_token_counter) {
            assert_ne!(ftm.frame_token_counter, 0, "Frame token overflow");
            ftm.frame_token_counter -= 1;
        }
        ftm.frame_token_occupied.insert(ftm.frame_token_counter);
        ftm.frame_token_counter
    }

    fn get_temp_token() -> u32 {
        let ftm = Self::get_instance();
        let token = ftm.frame_token_temp;
        assert!(token >= INVALID_FRAME_TOKEN_MASK, "Temp frame token overflow");
        ftm.frame_token_temp -= 1;
        token
    }

    fn get_instance() -> &'static mut Self {
        lazy_static!(
            static ref INSTANCE: FrameTokenManager = FrameTokenManager {
                frame_token_counter: INVALID_FRAME_TOKEN_MASK - 1,
                frame_token_occupied: HashSet::new(),
                frame_token_temp: u32::MAX,
            };
        );
        let p: *const FrameTokenManager = INSTANCE.deref();
        unsafe {
            let p: *mut FrameTokenManager = std::mem::transmute(p);
            p.as_mut().unwrap()
        }
    }
}

struct Continuation {
    token: u32,
    prev: HashSet<u32>,
    next: HashSet<u32>,
}

impl Continuation {
    fn new(token: u32) -> Self {
        Self {
            token,
            prev: HashSet::new(),
            next: HashSet::new(),
        }
    }
}

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

struct ModuleDuplicator {
    // map old node to new node
    callables: HashMap<*const CallableModule, CArc<CallableModule>>,
    nodes: HashMap<NodeRef, NodeRef>,
    blocks: HashMap<*const BasicBlock, Pooled<BasicBlock>>,
}

impl ModuleDuplicator {
    fn new() -> Self {
        Self {
            callables: HashMap::new(),
            nodes: HashMap::new(),
            blocks: HashMap::new(),
        }
    }
}

#[derive(Clone, Copy)]
struct SplitPossibility {
    possibly: bool,
    directly: bool,
    definitely: bool,
}

impl Default for SplitPossibility {
    fn default() -> Self {
        Self {
            possibly: false,
            directly: false,
            definitely: false,
        }
    }
}

#[derive(Clone, Copy)]
struct VisitState {
    bb: *const Pooled<BasicBlock>,
    start: NodeRef,
    end: NodeRef,
    present: NodeRef,
}

impl VisitState {
    fn new(bb: &Pooled<BasicBlock>, start: Option<NodeRef>, end: Option<NodeRef>, present: Option<NodeRef>) -> Self {
        let start = if let Some(node_ref_start) = start { node_ref_start } else { bb.first.get().next };
        let end = if let Some(node_ref_end) = end { node_ref_end } else { bb.last };
        let present = if let Some(node_ref_present) = present { node_ref_present } else { start };
        Self {
            bb: bb as *const Pooled<BasicBlock>,
            start,
            end,
            present,
        }
    }
    fn new_whole(bb: &Pooled<BasicBlock>) -> Self {
        Self::new(bb, None, None, None)
    }
    fn get_bb_ref(&self) -> &Pooled<BasicBlock> {
        unsafe { &*self.bb }
    }
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


struct SplitManager {
    old2new: Old2NewMap,
    new2old: New2OldMap,
    // map old node to new node
    coro_scopes: HashMap<u32, NodeRef>,
    continuations: HashMap<u32, Continuation>,
    split_possibility: HashMap<*const BasicBlock, SplitPossibility>,
}

impl SplitManager {
    fn new() -> Self {
        Self {
            old2new: Default::default(),
            new2old: Default::default(),
            coro_scopes: HashMap::new(),
            continuations: HashMap::new(),
            split_possibility: HashMap::new(),
        }
    }

    fn create_scope_builder_temp(&mut self, pools: CArc<ModulePools>) -> ScopeBuilder {
        ScopeBuilder::new(FrameTokenManager::get_temp_token(), pools)
    }


    fn coro_split_mark_in_bb(&mut self, bb: &Pooled<BasicBlock>) {
        let mut split_poss = self.split_possibility.entry(bb.as_ptr()).or_default().clone();
        bb.iter().for_each(|node_ref_present| {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
                Instruction::CoroSplitMark { token } => {
                    FrameTokenManager::register_frame_token(*token);
                    split_poss.possibly = true;
                    split_poss.directly = true;
                    split_poss.definitely = true;
                }
                // 3 Instructions that might contain coro split mark
                Instruction::Loop { body, cond } => {
                    self.coro_split_mark_in_bb(body);
                    let split_poss_body = self.split_possibility.get(&body.as_ptr()).unwrap();
                    split_poss.possibly |= split_poss_body.possibly;
                    split_poss.definitely |= split_poss_body.definitely;
                }
                Instruction::If { cond, true_branch, false_branch } => {
                    self.coro_split_mark_in_bb(true_branch);
                    self.coro_split_mark_in_bb(false_branch);
                    let split_poss_true = self.split_possibility.get(&true_branch.as_ptr()).unwrap();
                    let split_poss_false = self.split_possibility.get(&false_branch.as_ptr()).unwrap();
                    split_poss.possibly |= split_poss_true.possibly || split_poss_false.possibly;
                    split_poss.definitely |= split_poss_true.definitely && split_poss_false.definitely;
                }
                Instruction::Switch {
                    value: _,
                    default,
                    cases,
                } => {
                    self.coro_split_mark_in_bb(default);
                    let split_poss_default = self.split_possibility.get(&default.as_ptr()).unwrap().clone();
                    split_poss.possibly |= split_poss_default.possibly;
                    split_poss.definitely |= split_poss_default.definitely;
                    let mut split_poss_cases = SplitPossibility {
                        possibly: false,
                        directly: false,
                        definitely: true,
                    };
                    for SwitchCase { value: _, block } in cases.as_ref().iter() {
                        self.coro_split_mark_in_bb(block);
                        let split_poss_case = self.split_possibility.get(&block.as_ptr()).unwrap();
                        split_poss_cases.possibly |= split_poss_case.possibly;
                        split_poss_cases.definitely &= split_poss_case.definitely;
                    }
                    split_poss.possibly |= split_poss_default.possibly || split_poss_cases.possibly;
                    split_poss.definitely |= split_poss_default.definitely && split_poss_cases.definitely;
                }
                _ => {}
            }
        });
        self.split_possibility.insert(bb.as_ptr(), split_poss);
    }
    fn preprocess(&mut self, kernel: &KernelModule) -> ScopeBuilder {
        // TODO: where to process captures/args/shared?
        let bb = &kernel.module.entry;
        bb.nodes().iter().for_each(|node_ref_present| {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
                Instruction::CoroSplitMark { token } => {
                    FrameTokenManager::register_frame_token(*token);
                    self.continuations.insert(*token, Continuation::new(*token));
                }
                _ => {}
            }
        });

        let entry_token = FrameTokenManager::get_new_token();
        FrameTokenManager::register_frame_token(entry_token);
        self.continuations.insert(entry_token, Continuation::new(entry_token));
        ScopeBuilder {
            token: entry_token,
            finished: false,
            builder: IrBuilder::new(kernel.pools.clone()),
        }
    }

    fn build_scope(&mut self, pools: &CArc<ModulePools>, scope_builder: ScopeBuilder) {
        let frame_token = scope_builder.token;

        // build scope
        let scope_bb = scope_builder.builder.finish();
        let scope = Instruction::CoroScope {
            token: frame_token,
            body: scope_bb,
        };
        let scope_node = Node::new(CArc::new(scope), CArc::new(Type::Void));
        let scope_node_ref = new_node(pools, scope_node);
        self.coro_scopes.insert(frame_token, scope_node_ref);
    }
    fn build_scopes(&mut self, pools: &CArc<ModulePools>, mut sb_vec: Vec<ScopeBuilder>, force: bool) -> Vec<ScopeBuilder> {
        let mut sb_ans_vec = vec![];
        while let Some(scope_builder) = sb_vec.pop() {
            if scope_builder.finished || force {
                self.build_scope(pools, scope_builder);
            } else {
                sb_ans_vec.push(scope_builder);
            }
        }
        sb_ans_vec
    }

    fn record_continuation(&mut self, prev: u32, next: u32) {
        let mut continuation = self.continuations.get_mut(&prev).unwrap();
        continuation.token = prev;
        continuation.next.insert(next);
        let mut continuation = self.continuations.get_mut(&next).unwrap();
        continuation.token = next;
        continuation.prev.insert(prev);
    }

    fn visit_bb(&mut self, pools: &CArc<ModulePools>, visit_state: VisitState, mut scope_builder: ScopeBuilder) -> Vec<ScopeBuilder> {
        assert!(!scope_builder.finished);

        let mut node_ref_present = visit_state.start;
        while node_ref_present != visit_state.end {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
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
                    let sb_before = self.visit_coro_suspend(scope_builder, *token);
                    return vec![sb_before];
                }
                Instruction::CoroResume { token } => {
                    unreachable!("Split: CoroResume");
                }

                // 3 Instructions to be processed
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
                    let mut visit_result = self.visit_switch_case(pools, scope_builder, visit_state_after, value, cases, default);
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
            node_ref_present = node_ref_present.get().next;
        }
        vec![scope_builder]
    }
    fn visit_coro_split_mark(&mut self, mut sb_before: ScopeBuilder, token_next: u32, node_ref: NodeRef) -> (ScopeBuilder, ScopeBuilder) {
        let frame_token = sb_before.token;
        let mut builder = &mut sb_before.builder;

        // replace CoroSplitMark with CoroSuspend
        let coro_suspend = builder.coro_suspend(token_next);
        node_ref.replace_with(coro_suspend.get());

        // record continuation
        self.record_continuation(frame_token, token_next);

        // create a new scope builder for the next scope
        // the next frame must have a CoroResume
        let mut builder_next = IrBuilder::new(builder.pools.clone());
        builder_next.coro_resume(token_next);
        let sb_after = ScopeBuilder {
            token: token_next,
            finished: false,
            builder: builder_next,
        };
        sb_before.finished = true;
        (sb_before, sb_after)
    }
    fn visit_coro_suspend(&mut self, mut scope_builder: ScopeBuilder, token_next: u32) -> ScopeBuilder {
        // unimplemented!("Multi-visit unsupported");
        let frame_token = scope_builder.token;
        // let mut builder = &mut scope_builder.builder;

        // record continuation
        self.record_continuation(frame_token, token_next);

        scope_builder.finished = true;
        scope_builder
    }
    fn visit_branch_split(&mut self, pools: &CArc<ModulePools>, frame_token: u32,
                          branch: &Pooled<BasicBlock>, sb_after_vec: &mut Vec<ScopeBuilder>) -> ScopeBuilder {
        let scope_builder = ScopeBuilder::new(frame_token, pools.clone());
        let mut sb_vec = self.visit_bb(pools, VisitState::new_whole(branch), scope_builder);
        let sb_before_split = sb_vec.pop().unwrap();
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

        let split_poss_true = self.split_possibility.get(&true_branch.as_ptr()).unwrap().clone();
        let split_poss_false = self.split_possibility.get(&false_branch.as_ptr()).unwrap().clone();

        if !split_poss_true.possibly && !split_poss_false.possibly {
            // no split, duplicate the node
            self.duplicate_node(&mut scope_builder, visit_state.present);
            visit_result.split_possibly = false;
            visit_result.result.push(scope_builder);
        } else {
            // split in true/false_branch
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
            visit_result.split_possibly = true;
        }
        visit_result
    }
    fn visit_switch_case(&mut self, pools: &CArc<ModulePools>, mut scope_builder: ScopeBuilder,
                         visit_state: VisitState, value: &NodeRef, cases: &CBoxedSlice<SwitchCase>, default: &Pooled<BasicBlock>) -> VisitResult {
        // split in cases/default
        let cases_ref = cases.as_ref();
        let mut split_poss_case_vec = Vec::with_capacity(cases_ref.len());
        let mut split_poss_cases = SplitPossibility {
            possibly: false,
            definitely: true,
            directly: false,
        };
        let split_poss_default = self.split_possibility.get(&default.as_ptr()).unwrap().clone();
        for case in cases_ref.iter() {
            let split_poss_case = self.split_possibility.get(&case.block.as_ptr()).unwrap();
            split_poss_case_vec.push(split_poss_case.clone());
            split_poss_cases.possibly |= split_poss_case.possibly;
            split_poss_cases.definitely &= split_poss_case.definitely;
        }

        let mut visit_result = VisitResult::new();

        if !split_poss_cases.possibly && !split_poss_default.possibly {
            // no split, duplicate the node
            self.duplicate_node(&mut scope_builder, visit_state.present);
            visit_result.split_possibly = false;
        } else {
            // split in cases/default
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
            visit_result.split_possibly = true;
        }
        visit_result
    }
    fn visit_loop(&mut self, pools: &CArc<ModulePools>, mut scope_builder: ScopeBuilder,
                  visit_state: VisitState, body: &Pooled<BasicBlock>, cond: &NodeRef) -> VisitResult {
        let split_poss = self.split_possibility.get(&body.as_ptr()).unwrap();
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

    fn split(&mut self, kernel: &KernelModule) {
        // prepare
        let pools = &kernel.pools;
        let scope_builder = self.preprocess(kernel);
        let bb = &kernel.module.entry;

        // visit
        let mut sb_vec = self.visit_bb(pools, VisitState::new_whole(bb), scope_builder);
        while !sb_vec.is_empty() {
            let scope_builder = sb_vec.pop().unwrap();
            self.build_scope(pools, scope_builder);
        }
    }


    // duplicate functions
    fn find_duplicated_block(&mut self, frame_token: u32, bb: &Pooled<BasicBlock>) -> Pooled<BasicBlock> {
        self.old2new.blocks.get(&bb.as_ptr()).unwrap().get(&frame_token).unwrap().clone()
    }
    fn find_duplicated_node(&mut self, frame_token: u32, node: NodeRef) -> NodeRef {
        if !node.valid() { return INVALID_REF; }
        self.old2new.nodes.get(&node).unwrap().get(&frame_token).unwrap().clone()
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
        let mut node_ref_original = node_ref;
        while let Some(node_ref_t) = self.new2old.nodes.get(&node_ref) {
            node_ref_original = node_ref_t.clone();
        }
        self.old2new.nodes.entry(node_ref_original).or_default().insert(frame_token, dup_node_ref);
        self.new2old.nodes.entry(dup_node_ref).or_insert(node_ref_original);
        dup_node_ref
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
            Capture {
                node: self.duplicate_arg(frame_token, pools, capture.node.clone()),
                binding: capture.binding.clone(),
            }
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
        if let Some(copy) = self.old2new.callables.get(&callable.as_ptr()).unwrap().get(&frame_token) {
            return copy.clone();
        }
        let dup_callable = {
            let dup_args = self.duplicate_args(frame_token, &callable.pools, &callable.args);
            let dup_captures = self.duplicate_captures(frame_token, &callable.pools, &callable.captures);
            let dup_module = self.duplicate_module(frame_token, &callable.module);
            CallableModule {
                module: dup_module,
                ret_type: callable.ret_type.clone(),
                args: dup_args,
                captures: dup_captures,
                subroutines: callable.subroutines.clone(),
                subroutine_ids: callable.subroutine_ids.clone(),
                cpu_custom_ops: callable.cpu_custom_ops.clone(),
                pools: callable.pools.clone(),
            }
        };
        let dup_callable = CArc::new(dup_callable);
        let mut callable_original = callable.as_ptr();
        while let Some(callable_t) = self.new2old.callables.get(&callable_original) {
            callable_original = *callable_t;
        }
        self.old2new.callables.entry(callable_original).or_default().insert(frame_token, dup_callable.clone());
        self.new2old.callables.entry(dup_callable.as_ptr()).or_insert(callable_original);
        dup_callable
    }
    fn duplicate_block(&mut self, frame_token: u32, pools: &CArc<ModulePools>, bb: &Pooled<BasicBlock>) -> Pooled<BasicBlock> {
        assert!(!self.old2new.blocks.get(&bb.as_ptr()).unwrap().contains_key(&frame_token),
                "Basic block {:?} has already been duplicated", bb);
        let mut scope_builder = ScopeBuilder {
            token: frame_token,
            finished: false,
            builder: IrBuilder::new(pools.clone()),
        };
        bb.iter().for_each(|node| {
            self.duplicate_node(&mut scope_builder, node);
        });
        let dup_bb = scope_builder.builder.finish();
        // insert the duplicated block into the map
        let mut bb_original = bb.as_ptr();
        while let Some(bb_t) = self.new2old.blocks.get(&bb_original) {
            bb_original = *bb_t;
        }
        self.old2new.blocks.entry(bb_original).or_default().insert(frame_token, dup_bb.clone());
        self.new2old.blocks.entry(dup_bb.as_ptr()).or_insert(bb_original);
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
        assert!(!self.old2new.nodes.get(&node_ref).unwrap().contains_key(&frame_token),
                "Node {:?} has already been duplicated", node);
        let dup_node = match node.instruction.as_ref() {
            Instruction::Buffer => unreachable!("Buffer should be handled by duplicate_args"),
            Instruction::Bindless => unreachable!("Bindless should be handled by duplicate_args"),
            Instruction::Texture2D => unreachable!("Texture2D should be handled by duplicate_args"),
            Instruction::Texture3D => unreachable!("Texture3D should be handled by duplicate_args"),
            Instruction::Accel => unreachable!("Accel should be handled by duplicate_args"),
            Instruction::Shared => unreachable!("Shared should be handled by duplicate_shared"),
            Instruction::Uniform => unreachable!("Uniform should be handled by duplicate_args"),
            Instruction::Argument { .. } => unreachable!("Argument should be handled by duplicate_args"),
            Instruction::Local { init } => {
                let dup_init = self.find_duplicated_node(frame_token, *init);
                builder.local(dup_init)
            }
            Instruction::UserData(data) => builder.userdata(data.clone()),
            Instruction::Invalid => unreachable!("Invalid node should not appear in non-sentinel nodes"),
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
            Instruction::Suspend(..)
            | Instruction::CoroResume { .. }
            | Instruction::CoroScope { .. } => {
                unreachable!("Unexpected coroutine instruction in ModuleDuplicator::duplicate_node");
            }
        };
        // insert the duplicated node into the map
        let mut node_ref_original = node_ref;
        while let Some(node_ref_t) = self.new2old.nodes.get(&node_ref) {
            node_ref_original = node_ref_t.clone();
        }
        self.old2new.nodes.entry(node_ref_original).or_default().insert(frame_token, dup_node);
        self.new2old.nodes.entry(dup_node).or_insert(node_ref_original);
        dup_node
    }
}