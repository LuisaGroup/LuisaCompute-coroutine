use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use lazy_static::lazy_static;

use crate::{CArc, CBoxedSlice, Pooled};
use crate::ir::{SwitchCase, Instruction, BasicBlock, KernelModule, IrBuilder, ModulePools, NodeRef, Node, new_node, Type, Capture, INVALID_REF, CallableModule, Func, PhiIncoming, Module, CallableModuleRef};

struct FrameTokenManager {
    frame_token_counter: u32,
    frame_token_occupied: HashSet<u32>,
}

static INVALID_FRAME_TOKEN: u32 = u32::MAX;

impl FrameTokenManager {
    fn register_frame_token(&mut self, token: u32) {
        assert_ne!(token, INVALID_FRAME_TOKEN, "Invalid frame token");
        self.frame_token_occupied.insert(token);
    }
    fn get_new_token(&mut self) -> u32 {
        while self.frame_token_occupied.contains(&self.frame_token_counter) {
            self.frame_token_counter -= 1;
        }
        self.frame_token_occupied.insert(self.frame_token_counter);
        self.frame_token_counter
    }

    fn get_instance() -> &'static mut Self {
        lazy_static!(
            static ref INSTANCE: FrameTokenManager = FrameTokenManager {
                frame_token_counter: u32::MAX - 1,
                frame_token_occupied: HashSet::new(),
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
    finish: bool,
    builder: IrBuilder,
}

struct ModuleDuplicatorCtx {
    nodes: HashMap<NodeRef, NodeRef>,
    // map new node to old node
    blocks: HashMap<*const BasicBlock, Pooled<BasicBlock>>,
}

struct ModuleDuplicator {
    callables: HashMap<*const CallableModule, CArc<CallableModule>>,
    current: Option<ModuleDuplicatorCtx>,
}

impl ModuleDuplicator {
    fn new() -> Self {
        Self {
            callables: HashMap::new(),
            current: None,
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

struct SplitManager {
    module_duplicators: HashMap<u32, ModuleDuplicator>,
    old2new: HashMap<NodeRef, HashSet<(u32, NodeRef)>>,
    // map old node to new node
    coro_scopes: HashMap<u32, NodeRef>,
    continuations: HashMap<u32, Continuation>,
    split_possibility: HashMap<*const BasicBlock, SplitPossibility>,
}

impl SplitManager {
    fn new() -> Self {
        Self {
            module_duplicators: HashMap::new(),
            old2new: HashMap::new(),
            coro_scopes: HashMap::new(),
            continuations: HashMap::new(),
            split_possibility: HashMap::new(),
        }
    }

    fn with_context<T, F: FnOnce(&mut Self) -> T>(&mut self, frame_token: u32, f: F) -> T {
        let ctx = ModuleDuplicatorCtx {
            nodes: HashMap::new(),
            blocks: HashMap::new(),
        };
        let ret = f(self);
        self.module_duplicators.get_mut(&frame_token).unwrap().current = Some(ctx);
        ret
    }
    fn get_scope_builder_temp(&mut self, pools: &CArc<ModulePools>) -> ScopeBuilder {
        self.module_duplicators.remove(&INVALID_FRAME_TOKEN);
        self.continuations.remove(&INVALID_FRAME_TOKEN);
        let sb_temp = ScopeBuilder {
            token: INVALID_FRAME_TOKEN,
            finish: false,
            builder: IrBuilder::new(pools.clone()),
        };
        sb_temp
    }
    fn get_bb_temp(&mut self) -> Pooled<BasicBlock> {
        self.module_duplicators.remove(&INVALID_FRAME_TOKEN);
        self.continuations.remove(&INVALID_FRAME_TOKEN);
        let sb_temp = self.coro_scopes.remove(&INVALID_FRAME_TOKEN).unwrap();
        let inst = sb_temp.get().instruction.get_mut().unwrap();
        match inst {
            Instruction::CoroScope { token, body } => *body,
            _ => unreachable!("Invalid instruction for SplitManager::get_bb_temp"),
        }
    }

    // pub unsafe fn a() -> &'static mut i32 {
    //     static mut a_: i32 = 0;
    //     &mut a_
    // }


    fn coro_split_mark_in_bb(&mut self, bb: &Pooled<BasicBlock>) {
        let frame_token_manager = FrameTokenManager::get_instance();
        let split_poss = self.split_possibility.entry(bb.as_ptr()).or_default();
        bb.iter().for_each(|node_ref_present| {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
                Instruction::CoroSplitMark { token } => {
                    frame_token_manager.register_frame_token(*token);
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
                    let split_poss_default = self.split_possibility.get(&default.as_ptr()).unwrap();
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
    }
    fn preprocess(&mut self, kernel: &KernelModule) -> ScopeBuilder {
        // TODO: where to process captures/args/shared?
        let bb = &kernel.module.entry;
        let mut frame_token_manager = FrameTokenManager::get_instance();
        bb.nodes().iter().for_each(|node_ref_present| {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
                Instruction::CoroSplitMark { token } => {
                    frame_token_manager.register_frame_token(*token);
                    self.continuations.insert(*token, Continuation::new(*token));
                    self.module_duplicators.insert(*token, ModuleDuplicator::new());
                }
                _ => {}
            }
        });

        let entry_token = frame_token_manager.get_new_token();
        frame_token_manager.register_frame_token(entry_token);
        self.continuations.insert(entry_token, Continuation::new(entry_token));
        self.module_duplicators.insert(entry_token, ModuleDuplicator::new());
        ScopeBuilder {
            token: entry_token,
            finish: false,
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

    fn record_continuation(&mut self, prev: u32, next: u32) {
        let mut continuation = self.continuations.get_mut(&prev).unwrap();
        continuation.token = prev;
        continuation.next.insert(next);
        let mut continuation = self.continuations.get_mut(&next).unwrap();
        continuation.token = next;
        continuation.prev.insert(prev);
    }

    fn visit_bb(&mut self, pools: &CArc<ModulePools>, visit_state: VisitState, mut scope_builder: ScopeBuilder) -> Vec<ScopeBuilder> {
        let mut split_in_branch =
            |this: &mut SplitManager, branch: &Pooled<BasicBlock>, sb_next_vec: &mut Vec<ScopeBuilder>| -> Pooled<BasicBlock> {
                let sb_temp = this.get_scope_builder_temp(&pools);
                let mut sb_vec = this.visit_bb(pools, VisitState::new_whole(branch), sb_temp);
                let sb_before_split = sb_vec.pop().unwrap();
                sb_next_vec.extend(sb_vec);
                sb_before_split.builder.finish()
            };

        let mut node_ref_present = visit_state.start;
        while node_ref_present != visit_state.end {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
                // coroutine related instructions
                Instruction::CoroSplitMark { token } => {
                    // TODO: different behaviors for loop/if
                    let (sb_before, sb_next) = self.visit_coro_split_mark(scope_builder, *token, node_ref_present.clone());
                    let visit_state_next = VisitState::new(visit_state.get_bb_ref(), Some(node_ref_present.get().next), None, None);
                    let mut sb_vec = self.visit_bb(pools, visit_state_next, sb_next);
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
                    // TODO: cases that cannot be simplified
                    let split_poss = self.split_possibility.get(&body.as_ptr()).unwrap();
                    if split_poss.possibly {
                        /// split in body
                        let mut visit_state_next = visit_state.clone();
                        visit_state_next.present = node_ref_present;
                        return self.visit_loop_split(pools, scope_builder, visit_state_next, body, *cond);
                    } else {
                        self.duplicate_node(&mut scope_builder, node_ref_present);
                    }
                }
                Instruction::If { cond, true_branch, false_branch } => {
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
                    let split_poss_true = self.split_possibility.get(&true_branch.as_ptr()).unwrap();
                    let split_poss_false = self.split_possibility.get(&false_branch.as_ptr()).unwrap();
                    if !split_poss_true.possibly && !split_poss_false.possibly {
                        // no split, duplicate the node
                        self.duplicate_node(&mut scope_builder, node_ref_present);
                    } else {
                        // split in true/false_branch
                        let mut sb_next_vec = vec![];

                        // process true/false branch
                        let dup_true_branch = if split_poss_true.possibly {
                            split_in_branch(self, true_branch, &mut sb_next_vec)
                        } else {
                            self.duplicate_block(scope_builder.token, &scope_builder.builder.pools, true_branch)
                        };
                        let dup_false_branch = if split_poss_false.possibly {
                            split_in_branch(self, false_branch, &mut sb_next_vec)
                        } else {
                            self.duplicate_block(scope_builder.token, &scope_builder.builder.pools, false_branch)
                        };
                        let dup_cond = self.find_duplicated_node(scope_builder.token, *cond);
                        scope_builder.builder.if_(dup_cond, dup_true_branch, dup_false_branch);

                        // process next bb
                        if split_poss_true.definitely && split_poss_false.definitely {
                            self.build_scope(pools, scope_builder);
                        } else {
                            sb_next_vec.push(scope_builder);
                        }

                        let mut sb_ans_vec = vec![];
                        for sb_next in sb_next_vec {
                            let mut visit_state_next = visit_state.clone();
                            visit_state_next.present = node_ref_present.get().next;
                            sb_ans_vec.extend(self.visit_bb(pools, visit_state_next, sb_next));
                        }
                        return sb_ans_vec;
                    }
                }
                Instruction::Switch { value, cases, default } => {
                    // split in cases/default
                    let cases_ref = cases.as_ref();
                    let mut split_poss_cases = Vec::with_capacity(cases_ref.len());
                    let mut split_in_any_case = false;
                    let mut split_in_all_cases = true;
                    let split_in_default = self.split_possibly.contains(&default.as_ptr());
                    for case in cases_ref.iter() {
                        let split_poss_case = self.split_possibility.get(&case.block.as_ptr()).unwrap();
                        split_poss_cases.push(split_poss_case);
                    }

                    if !split_in_any_case && !split_in_default {
                        // no split, duplicate the node
                        self.duplicate_node(&mut scope_builder, node_ref_present);
                    } else {
                        // split in cases/default
                        let mut sb_next_vec = vec![];

                        // process cases
                        let dup_cases: Vec<_> = cases_ref.iter().enumerate().map(|(i, case)| {
                            let dup_block = if split_in_cases[i] {
                                split_in_branch(self, &case.block, &mut sb_next_vec)
                            } else {
                                self.duplicate_block(scope_builder.token, &scope_builder.builder.pools, &case.block)
                            };
                            SwitchCase {
                                value: case.value,
                                block: dup_block,
                            }
                        }).collect();
                        // process default
                        let dup_default = if split_in_default {
                            split_in_branch(self, default, &mut sb_next_vec)
                        } else {
                            self.duplicate_block(scope_builder.token, &scope_builder.builder.pools, default)
                        };
                        let dup_value = self.find_duplicated_node(scope_builder.token, *value);
                        scope_builder.builder.switch(dup_value, dup_cases.as_slice(), dup_default);

                        // process next bb
                        if split_in_all_cases && split_in_default {
                            self.build_scope(pools, scope_builder);
                        } else {
                            sb_next_vec.push(scope_builder);
                        }

                        let mut sb_ans_vec = vec![];
                        for sb_next in sb_next_vec {
                            let mut visit_state_next = visit_state.clone();
                            visit_state_next.present = node_ref_present.get().next;
                            sb_ans_vec.extend(self.visit_bb(pools, visit_state_next, sb_next));
                        }
                        return sb_ans_vec;
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
    fn visit_coro_split_mark(&mut self, mut scope_builder: ScopeBuilder, token_next: u32, node_ref: NodeRef) -> (ScopeBuilder, ScopeBuilder) {
        let frame_token = scope_builder.token;
        let mut builder = &mut scope_builder.builder;

        // replace CoroSplitMark with CoroSuspend
        let coro_suspend = builder.coro_suspend(token_next);
        node_ref.replace_with(coro_suspend.get());

        // record continuation
        self.record_continuation(frame_token, token_next);

        // create a new scope builder for the next scope
        // the next frame must have a CoroResume
        let mut builder_next = IrBuilder::new(builder.pools.clone());
        builder_next.coro_resume(token_next);
        let sb_next = ScopeBuilder {
            token: token_next,
            finish: false,
            builder: builder_next,
        };
        scope_builder.finish = true;
        (scope_builder, sb_next)
    }
    fn visit_coro_suspend(&mut self, mut scope_builder: ScopeBuilder, token_next: u32) -> ScopeBuilder {
        unimplemented!("Multi-visit unsupported");
        let frame_token = scope_builder.token;
        let mut builder = &mut scope_builder.builder;

        // record continuation
        self.record_continuation(frame_token, token_next);

        scope_builder.finish = true;
        scope_builder
    }
    fn visit_loop_split(&mut self, pools: &CArc<ModulePools>, mut scope_builder: ScopeBuilder,
                        visit_state: VisitState, body: &Pooled<BasicBlock>, cond: NodeRef) {
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
            let sb_temp = self.get_scope_builder_temp(&pools);
            let mut sb_vec = self.visit_bb(pools, VisitState::new_whole(body), sb_temp);
            let sb_body_before_split = sb_vec.pop().unwrap();
            let bb_body_before_split = sb_body_before_split.builder.finish();
            let dup_cond = self.find_duplicated_node(scope_builder.token, cond);
            scope_builder.builder.loop_(bb_body_before_split, dup_cond);
        }
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
    fn find_duplicated_block(&self, frame_token: u32, bb: &Pooled<BasicBlock>) -> Pooled<BasicBlock> {
        let ctx = self.module_duplicators.get(&frame_token).unwrap().current.as_ref().unwrap();
        ctx.blocks.get(&bb.as_ptr()).unwrap().clone()
    }
    fn find_duplicated_node(&self, frame_token: u32, node: NodeRef) -> NodeRef {
        if !node.valid() { return INVALID_REF; }
        let ctx = self.module_duplicators.get(&frame_token).unwrap().current.as_ref().unwrap();
        ctx.nodes.get(&node).unwrap().clone()
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
        self.old2new.entry(node_ref).or_default().insert((frame_token, dup_node_ref));
        self.module_duplicators.get_mut(&frame_token).unwrap()
            .current.as_mut().unwrap().nodes.insert(node_ref, dup_node_ref);
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
        if let Some(copy) = self.module_duplicators.get(&frame_token).unwrap()
            .callables.get(&callable.as_ptr()) {
            return copy.clone();
        }
        let dup_callable = self.with_context(frame_token, |this| {
            let dup_args = this.duplicate_args(frame_token, &callable.pools, &callable.args);
            let dup_captures = this.duplicate_captures(frame_token, &callable.pools, &callable.captures);
            let dup_module = this.duplicate_module(frame_token, &callable.module);
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
        });
        let dup_callable = CArc::new(dup_callable);
        self.module_duplicators.get_mut(&frame_token).unwrap().callables.insert(callable.as_ptr(), dup_callable.clone());
        dup_callable
    }
    fn duplicate_block(&mut self, frame_token: u32, pools: &CArc<ModulePools>, bb: &Pooled<BasicBlock>) -> Pooled<BasicBlock> {
        assert!(!self.module_duplicators.get_mut(&frame_token).unwrap().current.as_ref().unwrap().blocks.contains_key(&bb.as_ptr()),
                "Basic block {:?} has already been duplicated", bb);
        let mut scope_builder = ScopeBuilder {
            token: frame_token,
            finish: false,
            builder: IrBuilder::new(pools.clone()),
        };
        bb.iter().for_each(|node| {
            self.duplicate_node(&mut scope_builder, node);
        });
        let dup_bb = scope_builder.builder.finish();
        // insert the duplicated block into the map
        self.module_duplicators.get_mut(&frame_token).unwrap()
            .current.as_mut().unwrap().blocks.insert(bb.as_ptr(), dup_bb.clone());
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
        assert!(!self.module_duplicators.get_mut(&frame_token).unwrap().current.as_ref().unwrap().nodes.contains_key(&node_ref),
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
                // let dup_init = self.find_duplicated_node(frame, *init);
                // builder.local(dup_init)
                unimplemented!("Split: duplicate local")
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
        self.old2new.entry(node_ref).or_default().insert((frame_token, dup_node));
        self.module_duplicators.get_mut(&frame_token).unwrap()
            .current.as_mut().unwrap().nodes.insert(node_ref, dup_node);
        dup_node
    }
}