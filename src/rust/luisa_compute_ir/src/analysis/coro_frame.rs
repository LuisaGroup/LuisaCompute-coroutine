use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, format, Formatter};
use crate::analysis::frame_token_manager::FrameTokenManager;
use crate::display::DisplayIR;
use crate::ir::{BasicBlock, CallableModule, Instruction, KernelModule, ModulePools, NodeRef, SwitchCase, Type};
use crate::{CArc, CBoxedSlice, Pooled};
use crate::context::is_type_equal;

#[derive(Debug)]
pub(crate) struct Continuation {
    pub(crate) token: u32,
    pub(crate) prev: HashSet<u32>,
    pub(crate) next: HashSet<u32>,
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
    pub(crate) fn new(bb: &Pooled<BasicBlock>, start: Option<NodeRef>, end: Option<NodeRef>, present: Option<NodeRef>) -> Self {
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


#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ActiveVar {
    pub(crate) token: u32,

    pub(crate) defined: HashSet<NodeRef>,
    pub(crate) used: HashSet<NodeRef>,

    pub(crate) def_b: HashSet<NodeRef>,
    pub(crate) use_b: HashSet<NodeRef>,

    pub(crate) input: HashSet<NodeRef>,
    pub(crate) output: HashSet<NodeRef>,
}

impl ActiveVar {
    fn new(token: u32) -> Self {
        Self {
            token,
            defined: HashSet::new(),
            used: HashSet::new(),
            def_b: HashSet::new(),
            use_b: HashSet::new(),
            input: HashSet::new(),
            output: HashSet::new(),
        }
    }

    fn record_use(&mut self, node: NodeRef) {
        self.used.insert(node);
        if !self.defined.contains(&node) {
            self.use_b.insert(node);
        }
    }
    fn record_def(&mut self, node: NodeRef) {
        self.defined.insert(node);
        if !self.used.contains(&node) {
            self.def_b.insert(node);
        }
    }

    pub(crate) fn display(&self, display_ir: &DisplayIR) -> String {
        let mut output = format!("{:-^40}\n", format!("ActiveVar of {}", self.token));
        output += &format!("defined: {}\n", display_ir.display_existent_nodes(&self.defined));
        output += &format!("used: {}\n", display_ir.display_existent_nodes(&self.used));
        output += &format!("def_b: {}\n", display_ir.display_existent_nodes(&self.def_b));
        output += &format!("use_b: {}\n", display_ir.display_existent_nodes(&self.use_b));
        output += &format!("input: {}\n", display_ir.display_existent_nodes(&self.input));
        output += &format!("output: {}\n", display_ir.display_existent_nodes(&self.output));
        output += &"-".repeat(40);
        output += "\n";
        output
    }
}

pub(crate) struct CoroFrameAnalyser {
    pub(crate) continuations: HashMap<u32, Continuation>,
    pub(crate) active_vars: HashMap<u32, ActiveVar>,
    pub(crate) split_possibility: HashMap<*const BasicBlock, SplitPossibility>,
    visited_coro_split_mark: HashSet<NodeRef>,
    pub(crate) entry_token: u32,
}

impl CoroFrameAnalyser {
    pub(crate) fn new() -> Self {
        Self {
            continuations: HashMap::new(),
            active_vars: HashMap::new(),
            split_possibility: HashMap::new(),
            visited_coro_split_mark: HashSet::new(),
            entry_token: u32::MAX,
        }
    }

    pub(crate) fn analyse_callable(&mut self, callable: &CallableModule) {
        self.preprocess_bb(&callable.module.entry);

        let entry_token = FrameTokenManager::get_new_token();
        FrameTokenManager::register_frame_token(entry_token);
        self.entry_token = entry_token;
        let active_var = self.active_vars.entry(entry_token).or_insert(ActiveVar::new(entry_token));

        for arg in callable.args.as_ref() {
            active_var.record_def(*arg);
        }
        for capture in callable.captures.as_ref() {
            active_var.record_def(capture.node);
        }

        let visit_state = VisitState::new_whole(&callable.module.entry);
        let _ = self.visit_bb(FrameBuilder::new(entry_token), visit_state);

        self.calculate_frame();
    }

    pub(crate) fn display_active_vars(&self, display_ir: &DisplayIR) -> String {
        let mut output = String::new();
        let active_var_str: Vec<_> = self.active_vars.iter().map(|(token, active_var)| {
            active_var.display(display_ir)
        }).collect();
        output += &active_var_str.join("\n");
        output
    }

    fn calculate_frame(&mut self) {
        let mut changed = true;
        while changed {
            changed = false;
            for continuation in self.continuations.values() {
                let mut active_var = self.active_vars.get(&continuation.token).unwrap().clone();
                active_var.output.clear();
                let mut to_self = false;
                // OUT[B] = U IN[S] for all S in next[B]
                for token_next in continuation.next.iter() {
                    if continuation.token == *token_next {
                        to_self = true;
                    } else {
                        let active_var_next = self.active_vars.get_mut(&token_next).unwrap();
                        active_var.output.extend(active_var_next.input.iter());
                    }
                }
                if to_self {
                    active_var.output.extend(active_var.input.iter());
                }
                // IN[B] = use[B] U (OUT[B] - def[B])
                active_var.input = active_var.use_b.clone();
                let mut out_minus_def = active_var.output.clone();
                out_minus_def.retain(|node_ref| !active_var.def_b.contains(node_ref));
                active_var.input.extend(out_minus_def);

                // check if changed
                if active_var != *self.active_vars.get(&continuation.token).unwrap() {
                    changed = true;
                    self.active_vars.insert(continuation.token, active_var);
                }
            }
        }
    }
    fn preprocess_bb(&mut self, bb: &Pooled<BasicBlock>) {
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
                // 3 Instructions after CCF
                Instruction::Loop { body, cond } => {
                    self.preprocess_bb(body);
                    let split_poss_body = self.split_possibility.get(&body.as_ptr()).unwrap();
                    split_poss.possibly |= split_poss_body.possibly;
                    split_poss.definitely |= split_poss_body.definitely;
                }
                Instruction::If { cond, true_branch, false_branch } => {
                    self.preprocess_bb(true_branch);
                    self.preprocess_bb(false_branch);
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
                    self.preprocess_bb(default);
                    let split_poss_default = self.split_possibility.get(&default.as_ptr()).unwrap().clone();
                    split_poss.possibly |= split_poss_default.possibly;
                    split_poss.definitely |= split_poss_default.definitely;
                    let mut split_poss_cases = SplitPossibility {
                        possibly: false,
                        directly: false,
                        definitely: true,
                    };
                    for SwitchCase { value: _, block } in cases.as_ref().iter() {
                        self.preprocess_bb(block);
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

    fn visit_coro_split_mark(&mut self, mut fb_before: FrameBuilder, token_next: u32, node_ref: NodeRef) -> Vec<FrameBuilder> {
        let visited = self.visited_coro_split_mark.contains(&node_ref);
        self.visited_coro_split_mark.insert(node_ref);
        fb_before.finished = true;

        if visited {
            // coro suspend
            vec![fb_before]
        } else {
            // coro split mark
            // record continuation
            self.continuations.entry(fb_before.token).or_insert(Continuation::new(fb_before.token)).next.insert(token_next);
            self.continuations.entry(token_next).or_insert(Continuation::new(token_next)).prev.insert(fb_before.token);

            // create a new frame builder for the next scope
            let fb_next = FrameBuilder::new(token_next);
            vec![fb_before, fb_next]
        }
    }
    fn visit_branch_split(&mut self, frame_token: u32, branch: &Pooled<BasicBlock>,
                          sb_after_vec: &mut Vec<FrameBuilder>) -> FrameBuilder {
        let frame_builder = FrameBuilder::new(frame_token);
        let mut sb_vec = self.visit_bb(frame_builder, VisitState::new_whole(branch));
        let sb_before_split = sb_vec.remove(0);
        sb_after_vec.extend(sb_vec);
        sb_before_split
    }
    fn visit_loop(&mut self, mut frame_builder: FrameBuilder, visit_state: VisitState,
                  body: &Pooled<BasicBlock>, cond: &NodeRef) -> VisitResult {
        let mut visit_result = VisitResult::new();
        let mut fb_after_vec = vec![];

        let fb_body = self.visit_branch_split(frame_builder.token, body, &mut fb_after_vec);
        assert_eq!(fb_body.finished, self.split_possibility.get(&body.as_ptr()).unwrap().definitely);
        frame_builder.finished |= fb_body.finished;
        if !frame_builder.finished {
            self.active_vars.get_mut(&frame_builder.token).unwrap().record_use(*cond);
        }

        // process next bb
        fb_after_vec.insert(0, frame_builder);

        let mut visit_state_after = visit_state.clone();
        visit_state_after.present = visit_state.present.get().next;
        for mut fb_after in fb_after_vec {
            if fb_after.finished {
                visit_result.result.push(fb_after);
            } else {
                let mut temp_vec = vec![];
                fb_after = self.visit_branch_split(fb_after.token, body, &mut temp_vec);
                assert_eq!(temp_vec.len(), 0);
                if fb_after.finished {
                    visit_result.result.push(fb_after);
                } else {
                    visit_result.result.extend(self.visit_bb(fb_after, visit_state_after.clone()));
                }
            }
        }
        visit_result
    }
    fn visit_if(&mut self, mut frame_builder: FrameBuilder, visit_state: VisitState,
                true_branch: &Pooled<BasicBlock>, false_branch: &Pooled<BasicBlock>, cond: &NodeRef) -> VisitResult {
        // cond
        let active_var = self.active_vars.get_mut(&frame_builder.token).unwrap();
        active_var.record_use(*cond);

        let mut visit_result = VisitResult::new();

        // split in true/false_branch
        visit_result.split_possibly = true;
        let mut fb_after_vec = vec![];

        let mut all_branches_finished = true;
        // process true branch
        let fb_true = self.visit_branch_split(frame_builder.token, true_branch, &mut fb_after_vec);
        assert_eq!(fb_true.finished, self.split_possibility.get(&true_branch.as_ptr()).unwrap().definitely);
        all_branches_finished &= fb_true.finished;
        // process false branch
        let fb_false = self.visit_branch_split(frame_builder.token, false_branch, &mut fb_after_vec);
        assert_eq!(fb_false.finished, self.split_possibility.get(&false_branch.as_ptr()).unwrap().definitely);
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
                visit_result.result.extend(self.visit_bb(fb_after, visit_state_after.clone()));
            }
        }
        visit_result
    }
    fn visit_switch(&mut self, mut frame_builder: FrameBuilder, visit_state: VisitState,
                    value: &NodeRef, cases: &CBoxedSlice<SwitchCase>, default: &Pooled<BasicBlock>) -> VisitResult {
        // value
        let active_var = self.active_vars.get_mut(&frame_builder.token).unwrap();
        active_var.record_use(*value);

        let mut visit_result = VisitResult::new();
        let mut fb_after_vec = vec![];

        // process cases
        let mut all_branches_finished = true;
        cases.as_ref().iter().enumerate().for_each(|(i, case)| {
            let fb_case = self.visit_branch_split(frame_builder.token, &case.block, &mut fb_after_vec);
            assert_eq!(fb_case.finished, self.split_possibility.get(&case.block.as_ptr()).unwrap().definitely);
            all_branches_finished &= fb_case.finished;
        });
        // process default
        let fb_default = self.visit_branch_split(frame_builder.token, default, &mut fb_after_vec);
        assert_eq!(fb_default.finished, self.split_possibility.get(&default.as_ptr()).unwrap().definitely);
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
                visit_result.result.extend(self.visit_bb(fb_after, visit_state_after.clone()));
            }
        }
        visit_result
    }
    fn visit_bb_no_split(&mut self, mut frame_builder: FrameBuilder, visit_state: VisitState) -> FrameBuilder {
        let mut fb_vec = self.visit_bb(frame_builder, visit_state);
        assert_eq!(fb_vec.len(), 1);
        fb_vec.remove(0)
    }
    fn visit_bb(&mut self, mut frame_builder: FrameBuilder, mut visit_state: VisitState) -> Vec<FrameBuilder> {
        while visit_state.present != visit_state.end {
            let node = visit_state.present.get();
            let type_ = &node.type_;
            let instruction = node.instruction.as_ref();
            // println!("{:?}: {:?}", visit_state.present, instruction);

            let active_var = self.active_vars.entry(frame_builder.token).or_insert(ActiveVar::new(frame_builder.token));

            match instruction {
                Instruction::CoroSplitMark { token: token_next } => {
                    let mut fb_vec = self.visit_coro_split_mark(frame_builder, *token_next, visit_state.present);
                    return if fb_vec.len() == 2 {
                        // coro split mark
                        let visit_state_after = VisitState::new(visit_state.get_bb_ref(), Some(node.next), None, None);
                        let fb_after = fb_vec.pop().unwrap();
                        let fb_before = fb_vec.pop().unwrap();
                        let mut fb_vec = self.visit_bb(fb_after, visit_state_after);
                        fb_vec.insert(0, fb_before);
                        fb_vec
                    } else {
                        // coro suspend
                        assert_eq!(fb_vec.len(), 1);
                        fb_vec
                    };
                }

                // 3 Instructions after CCF
                Instruction::Loop { body, cond } => {
                    return self.visit_loop(frame_builder, visit_state.clone(), body, cond).result;
                }
                Instruction::If { cond, true_branch, false_branch } => {
                    return self.visit_if(frame_builder, visit_state.clone(), true_branch, false_branch, cond).result;
                }
                Instruction::Switch {
                    value: value,
                    default,
                    cases,
                } => {
                    return self.visit_switch(frame_builder, visit_state.clone(), value, cases, default).result;
                }

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
                    active_var.record_use(*init);
                    active_var.record_def(visit_state.present);
                }
                Instruction::UserData(_) => todo!(),
                Instruction::Const(_) => {
                    active_var.record_def(visit_state.present);
                }
                Instruction::Update { var, value } => {
                    active_var.record_use(*value);
                    active_var.record_def(*var);
                }
                Instruction::Call(func, args) => {
                    for arg in args.as_ref() {
                        active_var.record_use(*arg);
                    }
                    if !is_type_equal(type_, &Type::void()) {
                        active_var.record_def(visit_state.present);
                    }
                }
                Instruction::Phi(phi) => {
                    for phi_incoming in phi.as_ref() {
                        active_var.record_use(phi_incoming.value);
                    }
                    active_var.record_def(visit_state.present);
                }
                Instruction::Return(value) => {
                    if !is_type_equal(type_, &Type::void()) {
                        active_var.record_use(*value);
                    }
                }
                Instruction::GenericLoop { .. } => todo!(),
                Instruction::Break => {}
                Instruction::Continue => {}
                Instruction::AdScope { .. } => {}
                Instruction::RayQuery { .. } => todo!(),
                Instruction::AdDetach(_) => {}
                Instruction::Comment(_) => {}

                Instruction::CoroSuspend { .. }
                | Instruction::CoroResume { .. }
                | Instruction::CoroScope { .. } => unreachable!("{:?} should not be defined as statement directly", instruction),
            }
            visit_state.present = node.next;
        }
        vec![frame_builder]
    }
}