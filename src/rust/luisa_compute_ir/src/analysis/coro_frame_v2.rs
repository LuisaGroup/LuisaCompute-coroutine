// Detect vars use/def that appears twice: Fail

use crate::analysis::frame_token_manager::FrameTokenManager;
use crate::context::is_type_equal;
use crate::display::DisplayIR;
use crate::ir::{
    BasicBlock, CallableModule, Instruction, NodeRef, SwitchCase, Type,
};
use crate::{CBoxedSlice, Pooled};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Debug)]
pub(crate) struct Continuation {
    pub(crate) token: u32,
    pub(crate) prev: HashSet<u32>,
    pub(crate) next: HashSet<u32>,
}

impl Eq for Continuation {}

impl Ord for Continuation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token.cmp(&other.token)
    }
}

impl PartialEq<Self> for Continuation {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl PartialOrd<Self> for Continuation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.token.cmp(&other.token))
    }
}

impl Continuation {
    pub(crate) fn new(token: u32) -> Self {
        Self {
            token,
            prev: HashSet::new(),
            next: HashSet::new(),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct VisitState {
    pub(crate) bb: *const Pooled<BasicBlock>,
    pub(crate) start: NodeRef,
    pub(crate) end: NodeRef,
    pub(crate) present: NodeRef,
}

impl VisitState {
    pub(crate) fn new(
        bb: &Pooled<BasicBlock>,
        start: Option<NodeRef>,
        end: Option<NodeRef>,
        present: Option<NodeRef>,
    ) -> Self {
        let start = if let Some(node_ref_start) = start {
            node_ref_start
        } else {
            bb.first.get().next
        };
        let end = if let Some(node_ref_end) = end {
            node_ref_end
        } else {
            bb.last
        };
        let present = if let Some(node_ref_present) = present {
            node_ref_present
        } else {
            start
        };
        Self {
            bb: bb as *const Pooled<BasicBlock>,
            start,
            end,
            present,
        }
    }
    pub(crate) fn new_whole(bb: &Pooled<BasicBlock>) -> Self {
        Self::new(bb, None, None, None)
    }
    pub(crate) fn get_bb_ref(&self) -> &Pooled<BasicBlock> {
        unsafe { &*self.bb }
    }
}

#[derive(Debug)]
struct FrameBuilder {
    token: u32,
    finished: bool,
}

impl FrameBuilder {
    fn new(token: u32) -> Self {
        Self {
            token,
            finished: false,
        }
    }
}

#[derive(Debug)]
struct VisitResult {
    split_possibly: bool,
    result: Vec<FrameBuilder>,
}

impl VisitResult {
    fn new() -> Self {
        Self {
            split_possibly: true,
            result: vec![],
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct SplitPossibility {
    pub(crate) possibly: bool,
    pub(crate) directly: bool,
    pub(crate) definitely: bool,
}

#[derive(Clone)]
struct ActiveVar {
    var_existence: HashMap<NodeRef, HashSet<u32>>,
    frame_vars: HashMap<u32, HashSet<NodeRef>>,
}

impl ActiveVar {
    fn new() -> Self {
        Self {
            var_existence: HashMap::new(),
            frame_vars: HashMap::new(),
        }
    }

    fn record_use(&mut self, token: u32, node: NodeRef) {
        self.var_existence.entry(node).or_default().insert(token);
    }
    fn record_def(&mut self, token: u32, node: NodeRef) {
        self.var_existence.entry(node).or_default().insert(token);
    }

    pub(crate) fn display(&self, display_ir: &DisplayIR) -> String {
        let mut output = format!("{:-^40}\n", format!(" ActiveVar "));
        let display_fn = |(token, vars): (&u32, &HashSet<NodeRef>)| {
            let vars = display_ir.display_existent_nodes(vars);
            (*token, vars)
        };
        let frame_vars = BTreeMap::from_iter(self.frame_vars.iter().map(display_fn));
        for (token, vars) in frame_vars.iter() {
            output += &format!("Coro {}\n", token);
            output += &format!("    {}\n", vars);
        }
        output += &"-".repeat(40);
        output += "\n";
        output
    }

    fn calculate_frame(&mut self, tokens: &Vec<u32>) {
        let mut tokens: HashSet<u32> = HashSet::from_iter(tokens.iter().cloned());
        tokens.remove(&0);
        self.frame_vars.entry(0).or_default();
        for (var, var_existence) in self.var_existence.iter() {
            if var_existence.len() > 1 {
                for token in tokens.iter() {
                    self.frame_vars.entry(*token).or_default().insert(*var);
                }
            }
        }
    }
}

pub(crate) struct CoroFrameAnalyser {
    pub(crate) continuations: HashMap<u32, Continuation>,
    pub(crate) split_possibility: HashMap<*const BasicBlock, SplitPossibility>,
    pub(crate) entry_token: u32,
    visited_coro_split_mark: HashSet<NodeRef>,
    active_var: ActiveVar,
}

impl CoroFrameAnalyser {
    pub(crate) fn new() -> Self {
        Self {
            continuations: HashMap::new(),
            split_possibility: HashMap::new(),
            entry_token: u32::MAX,
            visited_coro_split_mark: HashSet::new(),
            active_var: ActiveVar::new(),
        }
    }

    pub(crate) fn analyse_callable(&mut self, callable: &CallableModule) {
        self.process_split_possibility(&callable.module.entry);

        let entry_token = FrameTokenManager::get_new_token();
        self.entry_token = entry_token;

        let visit_state = VisitState::new_whole(&callable.module.entry);
        let _ = self.visit_bb(FrameBuilder::new(entry_token), visit_state);

        self.calculate_frame(callable);
    }

    pub(crate) fn display_active_var(&self, display_ir: &DisplayIR) -> String {
        self.active_var.display(display_ir)
    }

    pub(crate) fn display_continuations(&self) -> String {
        let mut output = String::from("Continuations: {\n");
        let mut continuations = Vec::from_iter(self.continuations.values());
        // format for display
        continuations.sort();
        let continuation_str: Vec<_> = continuations.iter().map(|continuation| {
            let prev = BTreeSet::from_iter(continuation.prev.iter());
            let next = BTreeSet::from_iter(continuation.next.iter());
            format!("    {:?} => {} => {:?}", prev, continuation.token, next)
        }).collect();
        output += &continuation_str.join("\n");
        output += "\n}";
        output
    }

    pub(crate) fn frame_vars(&mut self, token: u32) -> &HashSet<NodeRef> {
        self.active_var.frame_vars.get(&token).unwrap()
    }

    fn calculate_frame(&mut self, callable: &CallableModule) {
        let args = &callable.args.as_ref().iter()
            .map(|arg| *arg).collect::<HashSet<_>>();
        let captures = &callable.captures.as_ref().iter()
            .map(|capture| capture.node).collect::<HashSet<_>>();
        let tokens = Vec::from_iter(self.continuations.keys().cloned());
        self.active_var.calculate_frame(&tokens);
        for (token, vars) in self.active_var.frame_vars.iter_mut() {
            let mut frame_vars = vars.clone();
            frame_vars = frame_vars.difference(args).cloned().collect::<HashSet<_>>();
            frame_vars = frame_vars.difference(captures).cloned().collect::<HashSet<_>>();
            vars.clone_from(&frame_vars);
        }
    }
    fn process_split_possibility(&mut self, bb: &Pooled<BasicBlock>) {
        let mut split_poss = self
            .split_possibility
            .entry(bb.as_ptr())
            .or_default()
            .clone();
        for node_ref_present in bb.iter() {
            let node = node_ref_present.get();
            let instruction = node.instruction.as_ref();
            match instruction {
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

                Instruction::CoroSplitMark { token } => {
                    FrameTokenManager::register_frame_token(*token);
                    split_poss.possibly = true;
                    split_poss.directly = true;
                    split_poss.definitely = true;
                }
                // 3 Instructions after CCF
                Instruction::Loop { body, cond } => {
                    self.process_split_possibility(body);
                    let split_poss_body = self.split_possibility.get(&body.as_ptr()).unwrap();
                    split_poss.possibly |= split_poss_body.possibly;
                    split_poss.definitely |= split_poss_body.definitely;
                }
                Instruction::If {
                    cond,
                    true_branch,
                    false_branch,
                } => {
                    self.process_split_possibility(true_branch);
                    self.process_split_possibility(false_branch);
                    let split_poss_true =
                        self.split_possibility.get(&true_branch.as_ptr()).unwrap();
                    let split_poss_false =
                        self.split_possibility.get(&false_branch.as_ptr()).unwrap();
                    split_poss.possibly |= split_poss_true.possibly || split_poss_false.possibly;
                    split_poss.definitely |= split_poss_true.definitely && split_poss_false.definitely;
                }
                Instruction::Switch {
                    value: _,
                    default,
                    cases,
                } => {
                    let mut split_poss_cases = SplitPossibility {
                        possibly: false,
                        directly: false,
                        definitely: true,
                    };
                    for SwitchCase { value: _, block } in cases.as_ref().iter() {
                        self.process_split_possibility(block);
                        let split_poss_case = self.split_possibility.get(&block.as_ptr()).unwrap();
                        split_poss_cases.possibly |= split_poss_case.possibly;
                        split_poss_cases.definitely &= split_poss_case.definitely;
                    }
                    self.process_split_possibility(default);
                    let split_poss_default = self
                        .split_possibility
                        .get(&default.as_ptr())
                        .unwrap()
                        .clone();
                    split_poss.possibly |= split_poss_default.possibly || split_poss_cases.possibly;
                    split_poss.definitely |= split_poss_default.definitely && split_poss_cases.definitely;
                }

                Instruction::CoroSuspend { .. }
                | Instruction::CoroResume { .. } => unreachable!("{:?} should not be defined as statement directly", instruction),
                Instruction::CoroRegister { token, value, var } => {}
                Instruction::Return(_) => {
                    // TODO
                }
                Instruction::GenericLoop { .. }
                | Instruction::Break
                | Instruction::Continue => {
                    // TODO
                    unreachable!("{:?} should no longer exist after CCF", instruction)
                }

                Instruction::Local { .. }
                | Instruction::UserData(_)
                | Instruction::Const(_)
                | Instruction::Update { .. }
                | Instruction::Call(_, _)
                | Instruction::Phi(_)
                | Instruction::RayQuery { .. }
                | Instruction::Print { .. }
                | Instruction::Comment(_) => {}

                Instruction::AdDetach(_)
                | Instruction::AdScope { .. } => unimplemented!("Auto-differentiate is not compatible with Coroutine"),
            }
        }
        self.split_possibility.insert(bb.as_ptr(), split_poss);
    }

    fn visit_coro_split_mark(
        &mut self,
        mut fb_before: FrameBuilder,
        token_next: u32,
        node_ref: NodeRef,
    ) -> Vec<FrameBuilder> {
        let visited = self.visited_coro_split_mark.contains(&node_ref);
        self.visited_coro_split_mark.insert(node_ref);
        fb_before.finished = true;

        // record continuation
        self.continuations.entry(fb_before.token).or_insert(Continuation::new(fb_before.token)).next.insert(token_next);
        self.continuations.entry(token_next).or_insert(Continuation::new(token_next)).prev.insert(fb_before.token);

        if visited {
            // coro suspend
            vec![fb_before]
        } else {
            // coro split mark
            // create a new frame builder for the next scope
            let fb_next = FrameBuilder::new(token_next);
            vec![fb_before, fb_next]
        }
    }
    fn visit_branch_split(
        &mut self,
        frame_token: u32,
        branch: &Pooled<BasicBlock>,
        sb_after_vec: &mut Vec<FrameBuilder>,
    ) -> FrameBuilder {
        let frame_builder = FrameBuilder::new(frame_token);
        let mut sb_vec = self.visit_bb(frame_builder, VisitState::new_whole(branch));
        let sb_before_split = sb_vec.remove(0);
        sb_after_vec.extend(sb_vec);
        sb_before_split
    }
    fn visit_loop(
        &mut self,
        mut frame_builder: FrameBuilder,
        visit_state: VisitState,
        body: &Pooled<BasicBlock>,
        cond: &NodeRef,
    ) -> VisitResult {
        let mut visit_result = VisitResult::new();
        let mut fb_after_vec = vec![];

        let fb_body = self.visit_branch_split(frame_builder.token, body, &mut fb_after_vec);
        assert_eq!(
            fb_body.finished,
            self.split_possibility
                .get(&body.as_ptr())
                .unwrap()
                .definitely
        );
        frame_builder.finished |= fb_body.finished;
        self.active_var.record_use(frame_builder.token, *cond);

        // process next bb
        fb_after_vec.insert(0, frame_builder);

        let mut visit_state_after = visit_state.clone();
        visit_state_after.present = visit_state.present.get().next;
        for mut fb_after in fb_after_vec {
            if fb_after.finished {
                visit_result.result.push(fb_after);
            } else {
                let mut temp_vec = vec![];
                self.active_var.record_use(fb_after.token, *cond);
                fb_after = self.visit_branch_split(fb_after.token, body, &mut temp_vec);
                assert_eq!(temp_vec.len(), 0);
                if fb_after.finished {
                    visit_result.result.push(fb_after);
                } else {
                    visit_result
                        .result
                        .extend(self.visit_bb(fb_after, visit_state_after.clone()));
                }
            }
        }
        visit_result
    }
    fn visit_if(
        &mut self,
        mut frame_builder: FrameBuilder,
        visit_state: VisitState,
        true_branch: &Pooled<BasicBlock>,
        false_branch: &Pooled<BasicBlock>,
        cond: &NodeRef,
    ) -> VisitResult {
        // cond
        self.active_var.record_use(frame_builder.token, *cond);

        let mut visit_result = VisitResult::new();

        let split_poss_true = self.split_possibility.get(&true_branch.as_ptr()).unwrap().clone();
        let split_poss_false = self.split_possibility.get(&false_branch.as_ptr()).unwrap().clone();

        // split in true/false_branch
        visit_result.split_possibly = split_poss_true.possibly || split_poss_false.possibly;
        let mut fb_after_vec = vec![];

        let mut all_branches_finished = true;
        // process true branch
        let fb_true = self.visit_branch_split(frame_builder.token, true_branch, &mut fb_after_vec);
        assert_eq!(fb_true.finished, split_poss_true.definitely,
                   "If true.finished = {}, split_poss_true.definitely = {}",
                   fb_true.finished, split_poss_true.definitely);
        all_branches_finished &= fb_true.finished;
        // process false branch
        let fb_false = self.visit_branch_split(frame_builder.token, false_branch, &mut fb_after_vec);
        assert_eq!(fb_false.finished, split_poss_false.definitely,
                   "If false.finished = {}, split_poss_false.definitely = {}",
                   fb_false.finished, split_poss_false.definitely);
        all_branches_finished &= fb_false.finished;
        frame_builder.finished |= all_branches_finished;

        // process next bb
        fb_after_vec.insert(0, frame_builder);

        let mut visit_state_after = visit_state.clone();
        visit_state_after.present = visit_state.present.get().next;
        for fb_after in fb_after_vec {
            if fb_after.finished {
                visit_result.result.push(fb_after);
            } else {
                visit_result
                    .result
                    .extend(self.visit_bb(fb_after, visit_state_after.clone()));
            }
        }
        visit_result
    }
    fn visit_switch(
        &mut self,
        mut frame_builder: FrameBuilder,
        visit_state: VisitState,
        value: &NodeRef,
        cases: &CBoxedSlice<SwitchCase>,
        default: &Pooled<BasicBlock>,
    ) -> VisitResult {
        // value
        self.active_var.record_use(frame_builder.token, *value);

        let mut visit_result = VisitResult::new();
        let mut fb_after_vec = vec![];

        // process cases
        let mut all_branches_finished = true;
        cases.as_ref().iter().enumerate().for_each(|(i, case)| {
            let fb_case =
                self.visit_branch_split(frame_builder.token, &case.block, &mut fb_after_vec);
            assert_eq!(
                fb_case.finished,
                self.split_possibility
                    .get(&case.block.as_ptr())
                    .unwrap()
                    .definitely
            );
            all_branches_finished &= fb_case.finished;
        });
        // process default
        let fb_default = self.visit_branch_split(frame_builder.token, default, &mut fb_after_vec);
        assert_eq!(
            fb_default.finished,
            self.split_possibility
                .get(&default.as_ptr())
                .unwrap()
                .definitely
        );
        all_branches_finished &= fb_default.finished;
        frame_builder.finished |= all_branches_finished;

        // process next bb
        fb_after_vec.insert(0, frame_builder);

        let mut visit_state_after = visit_state.clone();
        visit_state_after.present = visit_state.present.get().next;
        for fb_after in fb_after_vec {
            if fb_after.finished {
                visit_result.result.push(fb_after);
            } else {
                visit_result
                    .result
                    .extend(self.visit_bb(fb_after, visit_state_after.clone()));
            }
        }
        visit_result
    }
    fn visit_bb(&mut self, frame_builder: FrameBuilder, mut visit_state: VisitState) -> Vec<FrameBuilder> {
        while visit_state.present != visit_state.end {
            let node = visit_state.present.get();
            let type_ = &node.type_;
            let instruction = node.instruction.as_ref();
            let token = frame_builder.token;
            // println!("Token {}, Visit noderef {:?} : {:?}", frame_builder.token, visit_state.present.0, instruction);

            match instruction {
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

                Instruction::CoroSplitMark { token: token_next } => {
                    let mut fb_vec =
                        self.visit_coro_split_mark(frame_builder, *token_next, visit_state.present);
                    return if fb_vec.len() == 2 {
                        // coro split mark
                        let visit_state_after =
                            VisitState::new(visit_state.get_bb_ref(), Some(node.next), None, None);
                        let fb_after = fb_vec.pop().unwrap();
                        let fb_before = fb_vec.pop().unwrap();
                        let mut fb_vec = self.visit_bb(fb_after, visit_state_after);
                        fb_vec.insert(0, fb_before);
                        println!("fb_vec = {:?}", fb_vec);
                        fb_vec
                    } else {
                        // coro suspend
                        assert_eq!(fb_vec.len(), 1);
                        fb_vec
                    };
                }

                // 3 Instructions after CCF
                Instruction::Loop { body, cond } => {
                    return self
                        .visit_loop(frame_builder, visit_state.clone(), body, cond)
                        .result;
                }
                Instruction::If {
                    cond,
                    true_branch,
                    false_branch,
                } => {
                    return self
                        .visit_if(
                            frame_builder,
                            visit_state.clone(),
                            true_branch,
                            false_branch,
                            cond,
                        )
                        .result;
                }
                Instruction::Switch {
                    value: value,
                    default,
                    cases,
                } => {
                    return self
                        .visit_switch(frame_builder, visit_state.clone(), value, cases, default)
                        .result;
                }

                Instruction::Local { init } => {
                    self.active_var.record_use(token, *init);
                    self.active_var.record_def(token, visit_state.present);
                }
                Instruction::UserData(_) => todo!(),
                Instruction::Const(_) => {
                    self.active_var.record_def(token, visit_state.present);
                }

                // FIXME: 3 instructions: var undefined
                Instruction::Update { var, value } => {
                    self.active_var.record_use(token, *value);
                    self.active_var.record_def(token, *var);
                }
                Instruction::Call(func, args) => {
                    for arg in args.as_ref() {
                        self.active_var.record_use(token, *arg);
                    }
                    if !is_type_equal(type_, &Type::void()) {
                        self.active_var.record_def(token, visit_state.present);
                    }
                }
                Instruction::Phi(phi) => {
                    for phi_incoming in phi.as_ref() {
                        self.active_var.record_use(token, phi_incoming.value);
                    }
                    self.active_var.record_def(token, visit_state.present);
                }

                Instruction::Return(value) => {
                    if !is_type_equal(type_, &Type::void()) {
                        self.active_var.record_use(token, *value);
                    }
                }
                Instruction::GenericLoop { .. } => todo!(),
                Instruction::RayQuery { .. } => todo!(),

                Instruction::Break => {}
                Instruction::Continue => {}
                Instruction::AdScope { .. } => {}
                Instruction::AdDetach(_) => {}
                Instruction::Comment(_) => {}
                Instruction::Print { .. } => {}
                Instruction::CoroRegister { token: token_next, value, var } => {
                    // var <-> frame[token][index] <-> value
                    self.active_var.record_use(*token_next, *value);
                }
                Instruction::CoroSuspend { .. } | Instruction::CoroResume { .. } => unreachable!(
                    "{:?} should not be defined as statement directly",
                    instruction
                ),
            }
            visit_state.present = node.next;
        }
        vec![frame_builder]
    }
}
