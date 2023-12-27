// CoroFrame analysis for CoroGraph

// Const/Argument(by_value=true)/

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::ops::Index;
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


pub(crate) const STATE_INDEX_CORO_ID: usize = 0;
pub(crate) const STATE_INDEX_FRAME_TOKEN: usize = 1;

pub(crate) const STATE_INDEX_START: usize = 2;  // start slot index for other frame states


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
pub(crate) struct CoroFrame {
    pub(crate) input: BTreeMap<u32, HashSet<NodeRef>>,
    pub(crate) output: BTreeMap<(u32, u32), HashSet<NodeRef>>,
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

// Graph coloring algorithm for frame slot allocation
struct GraphColoring {
    // value node in IR
    vertices: HashSet<NodeRef>,
    // relationship of transferred together in CoroFrame
    edges: HashMap<NodeRef, HashSet<NodeRef>>,
    // record color counter of the same type
    color_of_type: HashMap<CArc<Type>, HashSet<usize>>,
    // color counter from 0
    color_counter: usize,
    // record color of each node
    color_of_node: HashMap<NodeRef, usize>,
}

impl GraphColoring {
    fn new() -> Self {
        Self {
            vertices: HashSet::new(),
            edges: HashMap::new(),
            color_of_type: HashMap::new(),
            color_counter: 0,
            color_of_node: HashMap::new(),
        }
    }

    fn add_edge(&mut self, from: NodeRef, to: NodeRef) {
        self.edges.entry(from).or_default().insert(to);
        self.edges.entry(to).or_default().insert(from);
    }
    fn add_vertex(&mut self, node: NodeRef) {
        self.vertices.insert(node);
    }
    fn greedy_coloring(&mut self) {
        // map NodeRef to its order in DisplayIR for stability
        // Make sure you have used DISPLAY_IR_DEBUG to display the whole module!
        let vertices: Vec<_> = self.vertices.iter().map(|node| {
            let node_number = unsafe { DISPLAY_IR_DEBUG.get().get(node) };
            (*node, node_number)
        }).collect();
        let mut vertices_number = BTreeSet::new();
        let mut vertice_number_to_node = BTreeMap::new();
        for (node, node_number) in vertices.iter() {
            vertices_number.insert(*node_number);
            vertice_number_to_node.insert(*node_number, *node);
        }

        // assign color to the first node
        let vertice_first = *vertices_number.first().unwrap();
        vertices_number.remove(&vertice_first);
        let vertice_first = vertice_number_to_node.get(&vertice_first).unwrap();
        let color_first = self.new_color();
        self.assign_color(*vertice_first, color_first);

        // assign color to remaining nodes greedily
        for vertice in vertices_number {
            let vertice = vertice_number_to_node.get(&vertice).unwrap();
            let mut color_used = HashSet::new();
            let edges = self.edges.entry(*vertice).or_default();
            for other in edges.iter() {
                if let Some(color_other) = self.color_of_node.get(other) {
                    color_used.insert(*color_other);
                }
            }
            let type_ = vertice.type_();
            let color_of_type = self.color_of_type.entry(type_.clone()).or_default().clone();
            let color_unused: Vec<_> = color_of_type.difference(&color_used).cloned().collect();
            let color_unused = BTreeSet::from_iter(color_unused);
            let color = if let Some(color) = color_unused.first() {
                *color
            } else {
                self.new_color()
            };
            self.assign_color(*vertice, color);
        }

        // map color counter to the index of frame slots
        let mut type2index = HashMap::new();
        let mut index: usize = STATE_INDEX_START;
        for (type_, colors) in self.color_of_type.iter_mut() {
            type2index.insert(type_.clone(), index);
            let index_next = index + colors.len();
            colors.clear();
            for i in index..index_next {
                colors.insert(i);
            }
            index = index_next;
        }
        let mut mapped = HashMap::new();
        for (node, color) in self.color_of_node.iter_mut() {
            if let Some(color_mapped) = mapped.get(color) {
                *color = *color_mapped;
            } else {
                let type_ = node.type_();
                let index = type2index.get_mut(&type_).unwrap();
                mapped.insert(*color, *index);
                *color = *index;
                *index += 1;
            }
        }
    }
    fn new_color(&mut self) -> usize {
        let color = self.color_counter;
        self.color_counter += 1;
        color
    }
    fn assign_color(&mut self, node: NodeRef, color: usize) {
        let type_ = node.type_();
        self.color_of_type.entry(type_.clone()).or_default().insert(color);
        self.color_of_node.insert(node, color);
    }
}

pub(crate) struct CoroFrameAnalyser {
    pub(crate) entry_token: u32,

    defined_default: HashSet<NodeRef>,
    active_vars: Vec<ActiveVar>,
    active_var_by_token: BTreeMap<u32, BTreeSet<usize>>,
    active_var_by_token_next: BTreeMap<u32, BTreeSet<usize>>,

    pub(crate) continuations: BTreeMap<u32, Continuation>,

    coro_frame: CoroFrame,
    pub(crate) frame_type: CArc<Type>,
    pub(crate) frame_fields: Vec<CArc<Type>>,
    pub(crate) node2frame_slot: HashMap<NodeRef, usize>,

    pub(crate) replayable_value_analysis: ReplayableValueAnalysis,
}

impl CoroFrameAnalyser {
    fn new(coro_graph: &CoroGraph) -> Self {
        FrameTokenManager::reset();
        for (token, coro_scope_ref) in coro_graph.tokens.iter() {
            unsafe { FrameTokenManager::register_frame_token(*token) }
        }
        let entry_token = FrameTokenManager::get_new_token();
        assert_eq!(entry_token, 0, "Entry token should be 0");
        Self {
            entry_token,
            defined_default: HashSet::new(),
            active_vars: vec![],
            active_var_by_token: BTreeMap::new(),
            active_var_by_token_next: BTreeMap::new(),
            continuations: BTreeMap::new(),
            coro_frame: CoroFrame::default(),
            frame_type: CArc::null(),
            frame_fields: vec![],
            node2frame_slot: HashMap::new(),
            replayable_value_analysis: ReplayableValueAnalysis::new(false),
        }
    }

    pub(crate) fn input_vars(&mut self, token: u32) -> &HashSet<NodeRef> {
        return self.coro_frame.input.get(&token).unwrap();
    }
    pub(crate) fn output_vars(&mut self, token: u32, token_next: u32) -> &HashSet<NodeRef> {
        return self.coro_frame.output.get(&(token, token_next)).unwrap();
    }

    fn analyse(&mut self, coro_graph: &CoroGraph) {
        let mut coro_scope_ref_vec = BTreeMap::new();
        for (token, scope) in coro_graph.tokens.iter() {
            coro_scope_ref_vec.insert(*token, *scope);
        }
        coro_scope_ref_vec.insert(self.entry_token, coro_graph.entry);
        for (&token, &coro_scope_ref) in coro_scope_ref_vec.iter() {
            let mut frame_builder = FrameBuilder::new(token, Some(&self.defined_default));
            let scope = coro_graph.scopes.get(coro_scope_ref.0).unwrap().instructions.clone();
            frame_builder = self.visit_bb(coro_graph, frame_builder, &scope);
            if self.active_var_by_token.entry(token).or_default().is_empty() {
                let index = self.active_vars.len();
                assert_eq!(frame_builder.active_var.token_next, INVALID_FRAME_TOKEN_MASK);
                self.active_vars.push(frame_builder.active_var.clone());
                self.active_var_by_token.entry(token).or_default().insert(index);
                self.active_var_by_token_next.entry(frame_builder.active_var.token_next).or_default().insert(index);
            }
        }
        let exit_index = self.active_vars.len();
        self.active_vars.push(ActiveVar::new(INVALID_FRAME_TOKEN_MASK));
        self.active_var_by_token.entry(INVALID_FRAME_TOKEN_MASK).or_default().insert(exit_index);
        self.active_var_by_token_next.entry(INVALID_FRAME_TOKEN_MASK).or_default().insert(exit_index);
    }

    pub(crate) fn token_vec(&self) -> Vec<u32> {
        Vec::from_iter(self.continuations.keys().cloned())
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
                        output.clone_from(input_next);
                    }
                }

                // IN[B] = use[B] U (OUT[B] - def[B])
                let mut input = HashSet::new();
                for index in self.active_var_by_token.get(token).unwrap() {
                    let active_var = self.active_vars.get(*index).unwrap();
                    let token_next = active_var.token_next;
                    input.extend(active_var.use_b.iter());
                    let mut out_minus_def = self.coro_frame.output.get(&(*token, token_next)).unwrap().clone();
                    out_minus_def.retain(|node_ref| !active_var.def_b.contains(node_ref));
                    input.extend(out_minus_def);
                }
                if input != *self.coro_frame.input.entry(*token).or_default() {
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
        let coro_id_type = register_type(coro_id_type);
        let token_type = Type::Primitive(Primitive::Uint32);
        let token_type = register_type(token_type);
        let mut frame_fields: Vec<CArc<Type>> = vec![coro_id_type, token_type];

        // other fields
        let mut graph_coloring = GraphColoring::new();
        for (token, input) in self.coro_frame.input.iter() {
            for i in input.iter() {
                graph_coloring.add_vertex(*i);
            }
            for i in input.iter() {
                for j in input.iter() {
                    if i != j {
                        graph_coloring.add_edge(*i, *j);
                    }
                }
            }
        }
        graph_coloring.greedy_coloring();

        // create frame type from fields
        let mut disc2count = BTreeMap::new();
        let mut disc2type = HashMap::new();
        for (type_, index) in graph_coloring.color_of_type.iter() {
            // TODO: sort strategy may be improved for type alignment & size
            let disc = (type_.alignment(), format!("{:?}", type_.as_ref()));
            disc2type.insert(disc.clone(), type_.clone());
            disc2count.insert(disc, index.len());
        }
        for (disc, count) in disc2count.iter() {
            let type_ = disc2type.get(disc).unwrap();
            for _ in 0..*count {
                frame_fields.push(type_.clone());
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
        self.node2frame_slot = graph_coloring.color_of_node.clone();
    }

    fn visit_bb(&mut self, coro_graph: &CoroGraph, mut frame_builder: FrameBuilder, bb: &Vec<CoroInstrRef>) -> FrameBuilder {
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
                let cond = coro_graph.instructions.get($cond.0).unwrap();
                if let CoroInstruction::Simple(cond) = cond {
                    record_use!(cond);
                } else {
                    panic!("{} condition should be CoroInstruction::Simple", $instr_str)
                }
            };
        }

        frame_builder.active_var.enter_scope();
        for instr_ref in bb.iter() {
            let instruction = coro_graph.instructions.get(instr_ref.0).unwrap();
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
                    frame_builder = self.visit_bb(coro_graph, frame_builder, body);
                }

                CoroInstruction::Loop {
                    body,
                    cond
                } => {
                    record_cond!(cond, "Loop");
                    frame_builder = self.visit_bb(coro_graph, frame_builder, body);
                }
                CoroInstruction::If {
                    cond,
                    true_branch,
                    false_branch
                } => {
                    record_cond!(cond, "If");
                    frame_builder = self.visit_bb(coro_graph, frame_builder, true_branch);
                    frame_builder = self.visit_bb(coro_graph, frame_builder, false_branch);
                }
                CoroInstruction::Switch {
                    cond,
                    cases,
                    default
                } => {
                    record_cond!(cond, "Switch");
                    for case in cases.iter() {
                        frame_builder = self.visit_bb(coro_graph, frame_builder, &case.body);
                    }
                    frame_builder = self.visit_bb(coro_graph, frame_builder, default);
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

pub(crate) struct CoroFrameAnalysis;

impl CoroFrameAnalysis {
    pub(crate) fn analyse(coro_graph: &CoroGraph, callable: &CallableModule) -> CoroFrameAnalyser {
        let callable_string = unsafe { DISPLAY_IR_DEBUG.get().display_ir_callable(callable) };    // for DEBUG
        println!("Before:\n{}", callable_string);    // for DEBUG

        let mut analyser = CoroFrameAnalyser::new(coro_graph);
        analyser.register_args_captures(callable);
        analyser.analyse(coro_graph);
        analyser.calculate_frame();

        // for DEBUG
        println!("CoroFrame Analysis");
        println!("node2frame_slot = {:#?}", analyser.node2frame_slot);
        println!("frame_type = {:#?}", analyser.frame_type);
        println!("Active Vars: {:#?}", analyser.active_vars);
        println!("CoroFrame: {:#?}", analyser.coro_frame);

        analyser
    }
}