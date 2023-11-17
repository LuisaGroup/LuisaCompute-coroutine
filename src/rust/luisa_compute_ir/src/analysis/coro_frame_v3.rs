// Detect vars use/def that appears twice: Fail

use crate::analysis::frame_token_manager::{FrameTokenManager, INVALID_FRAME_TOKEN_MASK};
use crate::context::is_type_equal;
use crate::display::DisplayIR;
use crate::ir::{BasicBlock, CallableModule, Instruction, INVALID_REF, NodeRef, SwitchCase, Type};
use crate::{CArc, CBoxedSlice, Pool, Pooled};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;

// Singleton pattern for DisplayIR
struct LazyDisplayIR {
    inner: Option<DisplayIR>,
}

impl LazyDisplayIR {
    const fn new() -> Self {
        Self { inner: None }
    }
}

impl LazyDisplayIR {
    fn get(&mut self) -> &mut DisplayIR {
        if self.inner.is_none() {
            self.inner = Some(DisplayIR::new());
        }
        self.inner.as_mut().unwrap()
    }
}

static mut DISPLAY_IR_DEBUG: LazyDisplayIR = LazyDisplayIR::new();  // for DEBUG

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
    active_var: ActiveVar,
}

impl FrameBuilder {
    fn new(token: u32, active_var: Option<ActiveVar>) -> Self {
        let active_var = if let Some(active_var) = active_var {
            active_var
        } else {
            ActiveVar::new(token)
        };
        Self {
            token,
            finished: false,
            active_var,
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

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SplitPossibility {
    pub(crate) possibly: bool,
    pub(crate) directly: bool,
    pub(crate) definitely: bool,
}

fn display_node_map2set(target: &HashMap<NodeRef, HashSet<i32>>) -> String {
    let output: Vec<_> = target.iter().map(|(node_ref, number)| unsafe {
        let node = DISPLAY_IR_DEBUG.get().var_str(node_ref);
        let number = BTreeSet::from_iter(number.iter().cloned());
        let number = format!("{:?}", number);
        let output = format!("{}: {}", node, number);
        (node, output)
    }).collect();
    let output = BTreeMap::from_iter(output);
    let output: Vec<_> = output.iter().map(|(_, output)| output.as_str()).collect();
    let output = output.join(", ");
    let output = format!("{{{}}}", output);
    output
}
fn display_node_map(target: &HashMap<NodeRef, i32>) -> String {
    let output: Vec<_> = target.iter().map(|(node_ref, number)| unsafe {
        let node = DISPLAY_IR_DEBUG.get().var_str(node_ref);
        let output = format!("{}: {}", node, number);
        (node, output)
    }).collect();
    let output = BTreeMap::from_iter(output);
    let output: Vec<_> = output.iter().map(|(_, output)| output.as_str()).collect();
    let output = output.join(", ");
    let output = format!("{{{}}}", output);
    output
}
fn display_node_set(target: &HashSet<NodeRef>) -> String {
    unsafe { DISPLAY_IR_DEBUG.get().vars_str(target) }
}

#[derive(Clone)]
struct VarScope {
    defined: HashMap<NodeRef, HashSet<i32>>,
}

impl Debug for VarScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VarScope")
            .field("defined", &display_node_map2set(&self.defined))
            .finish()
    }
}

impl VarScope {
    fn new() -> Self {
        Self {
            defined: HashMap::new(),
        }
    }
}

#[derive(Clone)]
struct ActiveVar {
    token: u32,
    token_next: u32,

    var_scopes: Vec<VarScope>,

    defined_count: HashMap<NodeRef, i32>,
    defined: HashMap<NodeRef, HashSet<i32>>,
    used: HashMap<NodeRef, HashSet<i32>>,
    def_b: HashSet<NodeRef>,
    use_b: HashSet<NodeRef>,
}

impl Debug for ActiveVar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let defined_count = display_node_map(&self.defined_count);
        let defined = display_node_map2set(&self.defined);
        let used = display_node_map2set(&self.used);
        let def_b = display_node_set(&self.def_b);
        let use_b = display_node_set(&self.use_b);
        f.debug_struct("ActiveVar")
            .field("token", &self.token)
            .field("token_next", &self.token_next)
            .field("var_scopes", &self.var_scopes)
            .field("defined_count", &defined_count)
            .field("defined", &defined)
            .field("used", &used)
            .field("def_b", &def_b)
            .field("use_b", &use_b)
            .finish()
    }
}

impl ActiveVar {
    fn new(token: u32) -> Self {
        Self {
            token,
            token_next: INVALID_FRAME_TOKEN_MASK,

            var_scopes: vec![],

            defined_count: HashMap::new(),
            defined: HashMap::new(),
            used: HashMap::new(),
            def_b: HashSet::new(),
            use_b: HashSet::new(),
        }
    }

    fn record_use(&mut self, node: NodeRef) {
        let defined_index = *self.defined_count.get(&node).unwrap_or(&-1);

        self.used.entry(node).or_default().insert(defined_index);
        if defined_index < 0 {
            self.use_b.insert(node);
        }
    }
    fn record_def(&mut self, node: NodeRef) {
        let defined_index = self.defined_count.entry(node).or_insert(-1);
        *defined_index += 1;
        let def_index = *defined_index;

        self.var_scopes.last_mut().unwrap().defined.entry(node).or_default().insert(def_index);
        self.defined.entry(node).or_default().insert(def_index);
        if !self.used.get(&node).unwrap_or(&HashSet::new()).contains(&def_index) {
            self.def_b.insert(node);
        }
    }

    fn enter_scope(&mut self) {
        self.var_scopes.push(VarScope::new());
    }
    fn exit_scope(&mut self) {
        let var_scope = self.var_scopes.pop().unwrap();
        for (node, defined_in_scope) in var_scope.defined.iter() {
            let defined = self.defined.entry(*node).or_default();
            assert!(defined_in_scope.len() <= defined.len());
            for defined_index in defined_in_scope.iter() {
                defined.remove(defined_index);
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
struct CoroFrame {
    input: HashMap<u32, HashSet<NodeRef>>,
    output: HashMap<(u32, u32), HashSet<NodeRef>>,
}

pub(crate) struct CoroFrameAnalyser {
    pub(crate) continuations: HashMap<u32, Continuation>,
    pub(crate) split_possibility: HashMap<*const BasicBlock, SplitPossibility>,
    pub(crate) entry_token: u32,
    visited_coro_split_mark: HashSet<NodeRef>,

    active_vars: Vec<ActiveVar>,
    active_var_by_token: HashMap<u32, HashSet<usize>>,
    active_var_by_token_next: HashMap<u32, HashSet<usize>>,

    coro_frame: CoroFrame,
}

impl CoroFrameAnalyser {
    pub(crate) fn new() -> Self {
        Self {
            continuations: HashMap::new(),
            split_possibility: HashMap::new(),
            entry_token: u32::MAX,
            visited_coro_split_mark: HashSet::new(),

            active_vars: Vec::new(),
            active_var_by_token: HashMap::new(),
            active_var_by_token_next: HashMap::new(),

            coro_frame: CoroFrame::default(),
        }
    }

    pub(crate) fn frame_vars(&mut self, token: u32) -> &HashSet<NodeRef> {
        return self.coro_frame.input.get(&token).unwrap();
    }

    pub(crate) fn analyse_callable(&mut self, callable: &CallableModule) {
        unsafe { DISPLAY_IR_DEBUG.get().display_ir_callable(callable); }  // for DEBUG

        self.process_split_possibility(&callable.module.entry);

        let entry_token = FrameTokenManager::get_new_token();
        self.entry_token = entry_token;

        let visit_state = VisitState::new_whole(&callable.module.entry);
        let fb_vec = self.visit_bb(FrameBuilder::new(entry_token, None), visit_state);

        self.calculate_frame(callable, fb_vec);
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

    fn calculate_frame(&mut self, callable: &CallableModule, mut fb_vec: Vec<FrameBuilder>) {
        let args = &callable.args.as_ref().iter()
            .map(|arg| *arg).collect::<HashSet<_>>();
        let captures = &callable.captures.as_ref().iter()
            .map(|capture| capture.node).collect::<HashSet<_>>();
        let tokens = Vec::from_iter(self.continuations.keys().cloned());

        for fb in fb_vec {
            let index = self.active_vars.len();
            assert_eq!(fb.token, fb.active_var.token);
            self.active_var_by_token.entry(fb.active_var.token).or_default().insert(index);
            self.active_var_by_token_next.entry(fb.active_var.token_next).or_default().insert(index);
            self.active_vars.push(fb.active_var);
        }


        let mut changed = true;
        while changed {
            changed = false;
            for (token, _) in self.continuations.iter() {
                // token -> *
                // OUT[B] = U IN[S] for all S in next[B]
                for index in self.active_var_by_token.get(token).unwrap() {
                    let active_var = self.active_vars.get(*index).unwrap();
                    let token_next = active_var.token_next;
                    let input_next = self.coro_frame.input.entry(token_next).or_default();
                    assert_eq!(*token, active_var.token);
                    let output = self.coro_frame.output.entry((*token, token_next)).or_default();
                    if output != input_next {
                        changed = true;
                        output.clone_from(input_next);
                    }
                }

                // IN[B] = use[B] U (OUT[B] - def[B])
                let input = self.coro_frame.input.entry(*token).or_default();
                for index in self.active_var_by_token.get(token).unwrap() {
                    let active_var = self.active_vars.get(*index).unwrap();
                    let token_next = active_var.token_next;
                    if !active_var.use_b.is_empty() {
                        changed = true;
                        input.extend(active_var.use_b.iter());
                    }
                    let mut out_minus_def = self.coro_frame.output.get(&(*token, token_next)).unwrap().clone();
                    out_minus_def.retain(|node_ref| !active_var.def_b.contains(node_ref));
                    if !out_minus_def.is_empty() {
                        changed = true;
                        input.extend(out_minus_def);
                    }
                }
            }
        }

        for (token, input) in self.coro_frame.input.iter_mut() {
            let mut input_var = input.clone();
            input_var = input_var.difference(args).cloned().collect::<HashSet<_>>();
            input_var = input_var.difference(captures).cloned().collect::<HashSet<_>>();
            input.clone_from(&input_var);
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
                    unreachable!("{:?} should be lowered in CCF", instruction)
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

        // active var
        fb_before.active_var.token_next = token_next;

        if visited {
            // coro suspend
            vec![fb_before]
        } else {
            // coro split mark
            // create a new frame builder for the next scope
            let fb_next = FrameBuilder::new(token_next, None);
            vec![fb_before, fb_next]
        }
    }
    fn visit_branch_split(
        &mut self,
        frame_builder: &FrameBuilder,
        branch: &Pooled<BasicBlock>,
        sb_after_vec: &mut Vec<FrameBuilder>,
    ) -> FrameBuilder {
        let frame_builder = FrameBuilder::new(frame_builder.token, Some(frame_builder.active_var.clone()));
        println!("visit_branch_split {:?}", frame_builder);
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

        let fb_body = self.visit_branch_split(&frame_builder, body, &mut fb_after_vec);
        assert_eq!(
            fb_body.finished,
            self.split_possibility
                .get(&body.as_ptr())
                .unwrap()
                .definitely
        );
        frame_builder.finished |= fb_body.finished;
        frame_builder.active_var.record_use(*cond);

        let mut visit_state_after = visit_state.clone();
        visit_state_after.present = visit_state.present.get().next;

        // process next bb
        if frame_builder.finished {
            visit_result.result.push(frame_builder);
        } else {
            visit_result.result.extend(self.visit_bb(frame_builder, visit_state_after.clone()));
        }

        for mut fb_after in fb_after_vec {
            if fb_after.finished {
                visit_result.result.push(fb_after);
            } else {
                let mut temp_vec = vec![];

                fb_after.active_var.record_use(*cond);
                fb_after = self.visit_branch_split(&fb_after, body, &mut temp_vec);
                assert_eq!(temp_vec.len(), 0);
                fb_after.active_var.record_use(*cond);

                visit_result.result.extend(self.visit_bb(fb_after, visit_state_after.clone()));
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
        frame_builder.active_var.record_use(*cond);

        let mut visit_result = VisitResult::new();

        let split_poss_true = self.split_possibility.get(&true_branch.as_ptr()).unwrap().clone();
        let split_poss_false = self.split_possibility.get(&false_branch.as_ptr()).unwrap().clone();

        // split in true/false_branch
        visit_result.split_possibly = split_poss_true.possibly || split_poss_false.possibly;
        let mut fb_after_vec = vec![];

        let mut all_branches_finished = true;
        // process true branch
        let fb_true = self.visit_branch_split(&frame_builder, true_branch, &mut fb_after_vec);
        assert_eq!(fb_true.finished, split_poss_true.definitely,
                   "If true.finished = {}, split_poss_true.definitely = {}",
                   fb_true.finished, split_poss_true.definitely);
        all_branches_finished &= fb_true.finished;
        // process false branch
        let fb_false = self.visit_branch_split(&frame_builder, false_branch, &mut fb_after_vec);
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
                visit_result.result.extend(self.visit_bb(fb_after, visit_state_after.clone()));
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
        frame_builder.active_var.record_use(*value);

        let mut visit_result = VisitResult::new();
        let mut fb_after_vec = vec![];

        // process cases
        let mut all_branches_finished = true;
        cases.as_ref().iter().enumerate().for_each(|(i, case)| {
            let fb_case =
                self.visit_branch_split(&frame_builder, &case.block, &mut fb_after_vec);
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
        let fb_default = self.visit_branch_split(&frame_builder, default, &mut fb_after_vec);
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
                visit_result.result.extend(self.visit_bb(fb_after, visit_state_after.clone()));
            }
        }
        visit_result
    }
    fn visit_bb(&mut self, mut frame_builder: FrameBuilder, mut visit_state: VisitState) -> Vec<FrameBuilder> {
        println!("visit_bb {:?}", frame_builder);
        let active_var = &mut frame_builder.active_var;
        active_var.enter_scope();

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
                Instruction::If {
                    cond,
                    true_branch,
                    false_branch,
                } => {
                    return self.visit_if(
                        frame_builder,
                        visit_state.clone(),
                        true_branch,
                        false_branch,
                        cond,
                    ).result;
                }
                Instruction::Switch {
                    value: value,
                    default,
                    cases,
                } => {
                    return self.visit_switch(frame_builder, visit_state.clone(), value, cases, default).result;
                }

                Instruction::Local { init } => {
                    active_var.record_use(*init);
                    active_var.record_def(visit_state.present);
                }
                Instruction::UserData(_) => todo!(),
                Instruction::Const(_) => {
                    active_var.record_def(visit_state.present);
                }

                // FIXME: 3 instructions: var undefined
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
                Instruction::RayQuery { .. } => todo!(),

                Instruction::Break => {}
                Instruction::Continue => {}
                Instruction::AdScope { .. } => {}
                Instruction::AdDetach(_) => {}
                Instruction::Comment(_) => {}
                Instruction::Print { .. } => {}
                Instruction::CoroRegister { token: token_next, value, var } => {
                    assert_eq!(*token_next, frame_builder.token);
                    // var <-> frame[token][index] <-> value
                    active_var.record_use(*value);
                }
                Instruction::CoroSuspend { .. } | Instruction::CoroResume { .. } => unreachable!(
                    "{:?} should not be defined as statement directly",
                    instruction
                ),
            }
            visit_state.present = node.next;
        }

        frame_builder.active_var.exit_scope();
        vec![frame_builder]
    }
}
