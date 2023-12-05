// CoroFrame analysis for CoroGraph

// Const/Argument(by_value=true)/

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use crate::analysis::{
    coro_graph::CoroGraph,
    frame_token_manager::{FrameTokenManager},
};
use crate::analysis::coro_graph::{CoroInstrRef, CoroInstruction, CoroScopeRef};
use crate::analysis::frame_token_manager::INVALID_FRAME_TOKEN_MASK;
use crate::analysis::utility::{display_node_map, display_node_set, value_copiable};
use crate::display::DisplayIR;
use crate::ir::{CallableModule, Func, Instruction, NodeRef};


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


// CoroFrame analysis

#[derive(Clone)]
struct VarScope {
    defined: HashSet<NodeRef>,
}

impl Debug for VarScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VarScope")
            .field("defined", &display_node_set(&self.defined))
            .finish()
    }
}

impl VarScope {
    fn new() -> Self {
        Self {
            defined: HashSet::new(),
        }
    }
}

#[derive(Clone)]
struct ActiveVar {
    token: u32,
    token_next: u32,

    var_scopes: Vec<VarScope>,

    defined: HashSet<NodeRef>,
    used: HashSet<NodeRef>,
    def_b: HashSet<NodeRef>,
    use_b: HashSet<NodeRef>,
}

impl Debug for ActiveVar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let defined = display_node_set(&self.defined);
        let used = display_node_set(&self.used);
        let def_b = display_node_set(&self.def_b);
        let use_b = display_node_set(&self.use_b);
        f.debug_struct("ActiveVar")
            .field("token", &self.token)
            .field("token_next", &self.token_next)
            .field("var_scopes", &self.var_scopes)
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

            defined: HashSet::new(),
            used: HashSet::new(),
            def_b: HashSet::new(),
            use_b: HashSet::new(),
        }
    }

    fn record_use(&mut self, node: NodeRef) {
        // let token = self.token; // for DEBUG
        self.used.insert(node);
        if !self.defined.contains(&node) {
            assert_eq!(self.use_b.insert(node), true);
        }
    }
    fn record_def(&mut self, node: NodeRef) {
        // let token = self.token; // for DEBUG
        assert_eq!(self.var_scopes.last_mut().unwrap().defined.insert(node), true);
        assert_eq!(self.defined.insert(node), true);
        if !self.used.contains(&node) {
            assert_eq!(self.def_b.insert(node), true);
        }
    }

    fn enter_scope(&mut self) {
        // let token = self.token; // for DEBUG
        self.var_scopes.push(VarScope::new());
    }
    fn exit_scope(&mut self) {
        // let token = self.token; // for DEBUG
        let var_scope = self.var_scopes.pop().unwrap();
        for node in var_scope.defined.iter() {
            assert_eq!(self.defined.remove(node), true);
        }
    }
}

struct CoroScopeInfo<'a> {
    coro_graph: &'a CoroGraph,

    token: u32,
    scope_index: CoroScopeRef,
    value_copiable: HashSet<NodeRef>,
}

impl<'a> CoroScopeInfo<'a> {
    fn new(coro_graph: &'a CoroGraph, token: u32, coro_scope_ref: CoroScopeRef) -> Self {
        Self {
            coro_graph,
            token,
            scope_index: coro_scope_ref,
            value_copiable: HashSet::new(),
        }
    }

    fn register_value_copiable(&mut self, node: NodeRef) {
        self.value_copiable.insert(node);
    }

    fn analyse_instr_source(&mut self, bb: &Vec<CoroInstrRef>) {
        let instr_map = &self.coro_graph.instructions;
        for instr_ref in bb.iter() {
            let instruction = instr_map.get(instr_ref.0).unwrap();
            match instruction {
                CoroInstruction::Simple(node_ref) => {
                    let instruction = node_ref.get().instruction.as_ref();
                    if value_copiable(node_ref) {
                        self.value_copiable.insert(*node_ref);
                    } else {
                        match instruction {
                            // possible uniform value source
                            Instruction::Local { init } => {
                                if self.value_copiable.contains(init) || value_copiable(init) {
                                    self.value_copiable.insert(*node_ref);
                                }
                            }
                            Instruction::Update { var, value } => {
                                // TODO: whether update should apply copiable?
                                if self.value_copiable.contains(value) || value_copiable(value) {
                                    self.value_copiable.insert(*var);
                                }
                            }

                            Instruction::Const(..)
                            | Instruction::Call(..) => { /* has been processed */ }
                            // end

                            Instruction::Phi(_) => {}

                            Instruction::Invalid => {
                                unreachable!("Invalid node should not appear in non-sentinel nodes");
                            }
                            Instruction::Buffer => {}
                            Instruction::Bindless => {}
                            Instruction::Texture2D => {}
                            Instruction::Texture3D => {}
                            Instruction::Accel => {}
                            Instruction::Shared => {}
                            Instruction::Uniform => {}
                            Instruction::Argument { .. } => {
                                unreachable!("{:?} should not appear in basic block", instruction)
                            }
                            Instruction::UserData(_) => {}

                            Instruction::Return(_)
                            | Instruction::GenericLoop { .. }
                            | Instruction::Break
                            | Instruction::Continue => {
                                unreachable!("Instruction {:?} should be lowered in CCF", instruction);
                            }

                            Instruction::Loop { .. }
                            | Instruction::If { .. }
                            | Instruction::Switch { .. } => {
                                unreachable!("Instruction {:?} is not CoroInstruction::Simple", instruction);
                            }

                            Instruction::AdScope { .. } => {}
                            Instruction::RayQuery { .. } => {}
                            Instruction::Print { .. } => {}
                            Instruction::AdDetach(_) => {}
                            Instruction::Comment(_) => {}

                            Instruction::CoroSplitMark { .. } => {
                                unreachable!("Instruction {:?} is not CoroInstruction::Simple", instruction);
                            }
                            Instruction::CoroSuspend { .. }
                            | Instruction::CoroResume { .. }
                            | Instruction::CoroRegister { .. } => {
                                unreachable!("Instruction {:?} unreachable in CoroFrame analysis", instruction);
                            }
                        }
                    }
                }
                CoroInstruction::Entry
                | CoroInstruction::EntryScope { .. } => {
                    unreachable!("Instruction {:?} won't exist after CoroGraph analysis", instruction);
                }

                CoroInstruction::ConditionStackReplay { .. } => {}
                CoroInstruction::MakeFirstFlag => {}
                CoroInstruction::SkipIfFirstFlag { .. } => {}
                CoroInstruction::ClearFirstFlag(_) => {}

                CoroInstruction::Loop {
                    body,
                    cond
                } => {
                    self.analyse_instr_source(body);
                }
                CoroInstruction::If {
                    cond,
                    true_branch,
                    false_branch
                } => {
                    self.analyse_instr_source(true_branch);
                    self.analyse_instr_source(false_branch);
                }
                CoroInstruction::Switch {
                    cond,
                    cases,
                    default
                } => {
                    for case in cases.iter() {
                        self.analyse_instr_source(&case.body);
                    }
                    self.analyse_instr_source(default);
                }
                CoroInstruction::Suspend { token } => {}
                CoroInstruction::Terminate => {}
            }
        }
    }
}

impl<'a> Debug for CoroScopeInfo<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let instr_src_copiable = display_node_set(&self.value_copiable);
        f.debug_struct("CoroScopeInfo")
            .field("token", &self.token)
            .field("instr_src_copiable", &instr_src_copiable)
            .finish()
    }
}


pub(crate) struct CoroFrameAnalyserImpl<'a> {
    coro_graph: &'a CoroGraph,

    pub(crate) entry_token: u32,
    coro_infos: BTreeMap<u32, CoroScopeInfo<'a>>,

    active_vars: Vec<ActiveVar>,
    active_var_by_token: BTreeMap<u32, BTreeSet<usize>>,
    active_var_by_token_next: BTreeMap<u32, BTreeSet<usize>>,
}

impl<'a> CoroFrameAnalyserImpl<'a> {
    pub(crate) fn new(coro_graph: &'a CoroGraph) -> Self {
        let mut coro_infos = BTreeMap::new();
        FrameTokenManager::reset();
        for (token, coro_scope_ref) in coro_graph.tokens.iter() {
            unsafe { FrameTokenManager::register_frame_token(*token) }
            coro_infos.insert(*token, CoroScopeInfo::new(coro_graph, *token, *coro_scope_ref));
        }
        let entry_token = FrameTokenManager::get_new_token();
        coro_infos.insert(entry_token, CoroScopeInfo::new(coro_graph, entry_token, coro_graph.entry));
        assert_eq!(entry_token, 0, "Entry token should be 0");
        Self {
            coro_graph,
            entry_token,
            coro_infos,
            active_vars: vec![],
            active_var_by_token: BTreeMap::new(),
            active_var_by_token_next: BTreeMap::new(),
        }
    }

    fn analyse_instr_source(&mut self) {
        for (token, coro_info) in self.coro_infos.iter_mut() {
            let coro_scope = self.coro_graph.scopes.get(coro_info.scope_index.0).unwrap();
            coro_info.analyse_instr_source(&coro_scope.instructions);
        }
    }

    fn register_args_captures(&mut self, callable: &CallableModule) {
        for (token, coro_info) in self.coro_infos.iter_mut() {
            for arg in callable.args.as_ref() {
                let instruction = arg.get().instruction.as_ref();
                match instruction {
                    Instruction::Argument { by_value } => {
                        if *by_value {
                            coro_info.register_value_copiable(*arg);
                        }
                    }
                    Instruction::Uniform => {
                        coro_info.register_value_copiable(*arg);
                    }
                    _ => {}
                }
            }
        }
    }
}

pub(crate) struct CoroFrameAnalyser;

impl<'a> CoroFrameAnalyser {
    pub(crate) fn analyse(coro_graph: &'a CoroGraph, callable: &CallableModule) {
        unsafe { DISPLAY_IR_DEBUG.get().display_ir_callable(callable); }    // for DEBUG

        let mut impl_ = CoroFrameAnalyserImpl::new(coro_graph);
        impl_.register_args_captures(callable);
        impl_.analyse_instr_source();

        for coro_info in impl_.coro_infos.values() {
            println!("{:?}", coro_info);
        }
        // todo!()
    }
}