// CoroFrame analysis for CoroGraph

// Const/Argument(by_value=true)/

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::process::exit;
use crate::analysis::{
    coro_graph::CoroGraph,
    frame_token_manager::{FrameTokenManager},
};
use crate::analysis::coro_graph::{CoroInstrRef, CoroInstruction, CoroScopeRef};
use crate::analysis::frame_token_manager::INVALID_FRAME_TOKEN_MASK;
use crate::analysis::utility::{DISPLAY_IR_DEBUG, display_node_map, display_node_map2set, display_node_set};
use crate::{CArc, CBoxedSlice};
use crate::analysis::replayable_values::ReplayableValueAnalysis;
use crate::context::{is_type_equal, register_type};
use crate::display::DisplayIR;
use crate::ir::{CallableModule, Func, Instruction, NodeRef, Primitive, StructType, Type, VectorElementType, VectorType};


// CoroFrame analysis

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

            var_scopes: vec![VarScope::new()],

            defined_count: HashMap::new(),
            defined: HashMap::new(),
            used: HashMap::new(),
            def_b: HashSet::new(),
            use_b: HashSet::new(),
        }
    }

    fn record_use(&mut self, replayable_value_analysis: &mut ReplayableValueAnalysis, node: NodeRef) {
        // let token = self.token; // for DEBUG
        let defined_index = *self.defined_count.get(&node).unwrap_or(&-1);
        self.used.entry(node).or_default().insert(defined_index);
        if defined_index < 0 && !replayable_value_analysis.detect(node) {
            self.use_b.insert(node);
        }
    }
    fn record_def(&mut self, node: NodeRef) {
        // let token = self.token; // for DEBUG
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
        // let token = self.token; // for DEBUG
        self.var_scopes.push(VarScope::new());
    }
    fn exit_scope(&mut self) {
        // let token = self.token; // for DEBUG
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

#[derive(Debug)]
struct FrameBuilder {
    token: u32,
    finished: bool,
    active_var: ActiveVar,
}

impl FrameBuilder {
    fn new(token: u32, defined_default: Option<&HashSet<NodeRef>>) -> Self {
        let mut active_var = ActiveVar::new(token);
        if defined_default.is_some() {
            let defined_default = defined_default.unwrap();
            for node in defined_default.iter() {
                active_var.record_def(*node);
            }
        };
        Self {
            token,
            finished: false,
            active_var,
        }
    }
}

#[derive(Default, Clone)]
struct CoroFrame {
    input: BTreeMap<u32, HashSet<NodeRef>>,
    output: BTreeMap<(u32, u32), HashSet<NodeRef>>,
    index: BTreeMap<u32, HashMap<NodeRef, usize>>,
}

impl Debug for CoroFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let input: Vec<_> = self.input.iter().map(|(token, nodes)| {
            let token = format!("{:?}", token);
            let nodes = display_node_set(nodes);
            format!("{}: {}", token, nodes)
        }).collect();
        let input = input.join(", ");
        let input = format!("{{{}}}", input);
        let output: Vec<_> = self.output.iter().map(|((token, token_next), nodes)| {
            let token = format!("{:?}", token);
            let token_next = format!("{:?}", token_next);
            let nodes = display_node_set(nodes);
            format!("({}, {}): {}", token, token_next, nodes)
        }).collect();
        let output = output.join(", ");
        let output = format!("{{{}}}", output);
        f.debug_struct("CoroFrame")
            .field("input", &input)
            .field("output", &output)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct Continuation {
    pub(crate) token: u32,
    pub(crate) prev: BTreeSet<u32>,
    pub(crate) next: BTreeSet<u32>,
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
            prev: BTreeSet::new(),
            next: BTreeSet::new(),
        }
    }
}

pub(crate) struct CoroFrameAnalyserImpl<'a> {
    coro_graph: &'a CoroGraph,

    pub(crate) entry_token: u32,

    defined_default: HashSet<NodeRef>,
    active_vars: Vec<ActiveVar>,
    active_var_by_token: BTreeMap<u32, BTreeSet<usize>>,
    active_var_by_token_next: BTreeMap<u32, BTreeSet<usize>>,

    continuations: BTreeMap<u32, Continuation>,

    coro_frame: CoroFrame,
    frame_type: CArc<Type>,
    frame_fields: Vec<CArc<Type>>,
    replayable_value_analysis: ReplayableValueAnalysis,
}

impl<'a> CoroFrameAnalyserImpl<'a> {
    pub(crate) fn new(coro_graph: &'a CoroGraph) -> Self {
        FrameTokenManager::reset();
        for (token, coro_scope_ref) in coro_graph.tokens.iter() {
            unsafe { FrameTokenManager::register_frame_token(*token) }
        }
        let entry_token = FrameTokenManager::get_new_token();
        assert_eq!(entry_token, 0, "Entry token should be 0");
        Self {
            coro_graph,
            entry_token,
            defined_default: HashSet::new(),
            active_vars: vec![],
            active_var_by_token: BTreeMap::new(),
            active_var_by_token_next: BTreeMap::new(),
            continuations: BTreeMap::new(),
            coro_frame: CoroFrame::default(),
            frame_type: CArc::null(),
            frame_fields: vec![],
            replayable_value_analysis: ReplayableValueAnalysis::new(false),
        }
    }

    fn analyse(&mut self) {
        let mut coro_scope_ref_vec = BTreeMap::new();
        for (token, scope) in self.coro_graph.tokens.iter() {
            coro_scope_ref_vec.insert(*token, *scope);
        }
        coro_scope_ref_vec.insert(self.entry_token, self.coro_graph.entry);
        for (&token, &coro_scope_ref) in coro_scope_ref_vec.iter() {
            let mut frame_builder = FrameBuilder::new(token, Some(&self.defined_default));
            let scope = self.coro_graph.scopes.get(coro_scope_ref.0).unwrap().instructions.clone();
            frame_builder = self.visit_bb(frame_builder, &scope);
            if self.active_var_by_token.entry(token).or_default().is_empty() {
                let index = self.active_vars.len();
                assert_eq!(frame_builder.active_var.token_next, INVALID_FRAME_TOKEN_MASK);
                self.active_vars.push(frame_builder.active_var.clone());
                self.active_var_by_token.entry(token).or_default().insert(index);
            }
        }
    }

    fn register_args_captures(&mut self, callable: &CallableModule) {
        for arg in callable.args.as_ref() {
            assert_eq!(self.defined_default.insert(*arg), true);
        }
        for capture in callable.captures.as_ref() {
            assert_eq!(self.defined_default.insert(capture.node), true);
        }
    }

    fn calculate_frame(&mut self) {
        let mut changed = true;
        let token_vec = self
            .continuations
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        while changed {
            changed = false;
            for token in token_vec.iter() {
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
                let mut input = self.coro_frame.input.entry(*token).or_default().clone();
                for index in self.active_var_by_token.get(token).unwrap() {
                    let active_var = self.active_vars.get(*index).unwrap();
                    let token_next = active_var.token_next;
                    input.extend(active_var.use_b.iter());
                    let mut out_minus_def = self.coro_frame.output.get(&(*token, token_next)).unwrap().clone();
                    out_minus_def.retain(|node_ref| !active_var.def_b.contains(node_ref));
                    input.extend(out_minus_def);
                }
                if input != *self.coro_frame.input.get(token).unwrap() {
                    changed = true;
                    self.coro_frame.input.insert(*token, input);
                }
            }
        }

        // calculate frame type
        // coro id/token for slot 1/2
        let coro_id_type = Type::Vector(VectorType {
            element: VectorElementType::Scalar(Primitive::Uint32),
            length: 3,
        });
        let token_type = Type::Primitive(Primitive::Uint32);
        let mut frame_fields: Vec<CArc<Type>> = vec![CArc::new(coro_id_type), CArc::new(token_type)];

        let mut fields_type: BTreeMap<Type, usize> = BTreeMap::new();
        for token in token_vec.iter() {
            let frame_vars: Vec<NodeRef> = self.coro_frame.input.get(token).unwrap()
                .iter().map(ToOwned::to_owned).collect();
            let mut fields_type_temp: BTreeMap<Type, usize> = BTreeMap::new();
            for node_ref in frame_vars.iter() {
                let type_ = node_ref.type_().as_ref();
                let count = fields_type_temp.entry(type_.clone()).or_default();
                 *count += 1;
            }
            for (type_, count_temp) in fields_type_temp.iter() {
                let count = fields_type.entry(type_.clone()).or_default();
                *count = (*count).max(*count_temp);
            }
        }

        let mut free_fields: HashMap<CArc<Type>, Vec<usize>> = HashMap::new();
        let mut index_counter: usize = frame_fields.len();
        for (type_, count) in fields_type.iter() {
            for _ in 0..*count {
                let index = index_counter;
                index_counter += 1;
                let type_ = CArc::new(type_.clone());
                frame_fields.push(type_.clone());
                free_fields.entry(type_).or_default().push(index);
            }
        }

        let alignment = frame_fields
            .iter()
            .map(|type_| type_.alignment())
            .max()
            .unwrap();
        let size = frame_fields.iter().map(|type_| type_.size()).sum();
        self.frame_fields = frame_fields.clone();
        self.frame_type = register_type(Type::Struct(StructType {
            fields: CBoxedSlice::new(frame_fields),
            alignment,
            size,
        }));

        // allocate frame slots
        todo!()
    }

    fn visit_bb(&mut self, mut frame_builder: FrameBuilder, bb: &Vec<CoroInstrRef>) -> FrameBuilder {
        macro_rules! record_use {
            ($node: expr) => {
                frame_builder.active_var.record_use(&mut self.replayable_value_analysis, *$node);
            };
        }
        macro_rules! record_def {
            ($node: expr) => {
                frame_builder.active_var.record_def(*$node);
            };
        }
        macro_rules! record_cond {
            ($cond: expr, $instr_str: expr) => {
                let cond = self.coro_graph.instructions.get($cond.0).unwrap();
                if let CoroInstruction::Simple(cond) = cond {
                    record_use!(cond);
                } else {
                    panic!("{} condition should be CoroInstruction::Simple", $instr_str)
                }
            };
        }

        frame_builder.active_var.enter_scope();
        for instr_ref in bb.iter() {
            let instruction = self.coro_graph.instructions.get(instr_ref.0).unwrap();
            match instruction {
                CoroInstruction::Simple(node_ref) => {
                    let instruction = node_ref.get().instruction.as_ref();
                    let type_ = node_ref.type_();
                    match instruction {
                        // possible copiable value source
                        Instruction::Local { init } => {
                            record_use!(init);
                            record_def!(node_ref);
                        }
                        Instruction::Const(..) => {
                            record_def!(node_ref);
                        }
                        Instruction::Call(func, args) => {
                            for arg in args.iter() {
                                // FIXME: arg can Read/Write, we only consider Read here (fixe later)
                                record_use!(arg);
                            }
                            if !is_type_equal(type_, &Type::void()) {
                                record_def!(node_ref);
                            }
                        }

                        Instruction::Update { var, value } => {
                            record_use!(value);
                            record_def!(var);
                        }
                        // end

                        Instruction::Phi(phi) => {
                            for phi in phi.as_ref() {
                                record_use!(&phi.value);
                            }
                            record_def!(node_ref);
                        }

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

                        Instruction::Return(_) => {
                            unreachable!("Instruction {:?} should be lowered in CoroGraph", instruction);
                        }
                        Instruction::GenericLoop { .. }
                        | Instruction::Break
                        | Instruction::Continue => {
                            unreachable!("Instruction {:?} should be lowered in CCF", instruction);
                        }

                        Instruction::Loop { .. }
                        | Instruction::If { .. }
                        | Instruction::Switch { .. } => {
                            unreachable!("Instruction {:?} is not CoroInstruction::Simple", instruction);
                        }

                        Instruction::UserData(_) => {}
                        Instruction::AdScope { .. } => {}
                        Instruction::RayQuery { .. } => {}
                        Instruction::Print { .. } => {}
                        Instruction::AdDetach(_) => {}
                        Instruction::Comment(_) => {}

                        Instruction::CoroSplitMark { .. } => {
                            unreachable!("Instruction {:?} is not CoroInstruction::Simple", instruction);
                        }
                        Instruction::CoroSuspend { .. }
                        | Instruction::CoroResume { .. } => {
                            unreachable!("Instruction {:?} unreachable in CoroFrame analysis", instruction);
                        }
                        Instruction::CoroRegister { .. } => {
                            todo!()
                        }
                    }
                }
                CoroInstruction::Entry
                | CoroInstruction::EntryScope { .. } => {
                    unreachable!("Instruction {:?} unreachable in CoroFrame analysis", instruction);
                }

                CoroInstruction::ConditionStackReplay { items } => {
                    for item in items.iter() {
                        record_def!(&item.node);
                    }
                }

                CoroInstruction::MakeFirstFlag => {}
                CoroInstruction::ClearFirstFlag(_) => {}
                CoroInstruction::SkipIfFirstFlag { flag, body } => {
                    frame_builder = self.visit_bb(frame_builder, body);
                }

                CoroInstruction::Loop {
                    body,
                    cond
                } => {
                    record_cond!(cond, "Loop");
                    frame_builder = self.visit_bb(frame_builder, body);
                }
                CoroInstruction::If {
                    cond,
                    true_branch,
                    false_branch
                } => {
                    record_cond!(cond, "If");
                    frame_builder = self.visit_bb(frame_builder, true_branch);
                    frame_builder = self.visit_bb(frame_builder, false_branch);
                }
                CoroInstruction::Switch {
                    cond,
                    cases,
                    default
                } => {
                    record_cond!(cond, "Switch");
                    for case in cases.iter() {
                        frame_builder = self.visit_bb(frame_builder, &case.body);
                    }
                    frame_builder = self.visit_bb(frame_builder, default);
                }
                CoroInstruction::Suspend { token: token_next } => {
                    let token = frame_builder.token;

                    // record continuation
                    let continuation = self.continuations.entry(token).or_insert(Continuation::new(token));
                    continuation.next.insert(*token_next);
                    let continuation = self.continuations.entry(*token_next).or_insert(Continuation::new(*token_next));
                    continuation.prev.insert(token);

                    // record active var
                    frame_builder.active_var.token_next = *token_next;
                    assert_eq!(token, frame_builder.active_var.token);
                    let index = self.active_vars.len();
                    self.active_var_by_token.entry(token).or_default().insert(index);
                    self.active_var_by_token_next.entry(*token_next).or_default().insert(index);
                    self.active_vars.push(frame_builder.active_var.clone());
                    frame_builder.active_var.token_next = INVALID_FRAME_TOKEN_MASK;
                }
                CoroInstruction::Terminate => {}
            }
        }
        frame_builder.active_var.exit_scope();
        frame_builder
    }
}

pub(crate) struct CoroFrameAnalyser;

impl<'a> CoroFrameAnalyser {
    pub(crate) fn analyse(coro_graph: &'a CoroGraph, callable: &CallableModule) {
        let callable_string = unsafe { DISPLAY_IR_DEBUG.get().display_ir_callable(callable) };    // for DEBUG
        println!("Before:\n{}", callable_string);    // for DEBUG

        let mut impl_ = CoroFrameAnalyserImpl::new(coro_graph);
        impl_.register_args_captures(callable);
        impl_.analyse();
        impl_.calculate_frame();
        println!("CoroFrame Analysis");                             // for DEBUG
        println!("Active Vars: {:#?}", impl_.active_vars);     // for DEBUG
        println!("CoroFrame: {:#?}", impl_.coro_frame);
        // todo!()
    }
}