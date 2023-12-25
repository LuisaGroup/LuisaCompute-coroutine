use crate::analysis::const_eval::ConstEval;
use crate::analysis::coro_graph::{CoroGraph, CoroInstrRef, CoroInstruction, CoroScopeRef};
use crate::analysis::coro_transfer_graph::CoroTransferGraph;
use crate::analysis::utility::AccessTree;
use crate::ir::{BasicBlock, Instruction, NodeRef};
use std::collections::{BTreeMap, HashMap, HashSet};

pub(crate) struct CoroFrame {}

struct CoroScopeFootprints {
    pub locals: HashSet<NodeRef>,
    pub live_refs: AccessTree,
}

impl CoroScopeFootprints {
    fn new() -> CoroScopeFootprints {
        CoroScopeFootprints {
            locals: HashSet::new(),
            live_refs: AccessTree::new(),
        }
    }
}

struct CoroFrameBuilder<'a> {
    graph: &'a CoroGraph,
    transfer_graph: &'a CoroTransferGraph,
    node_stable_indices: HashMap<NodeRef, u32>,
    scope_footprints: HashMap<CoroScopeRef, CoroScopeFootprints>,
    current_scope: Option<CoroScopeRef>,
}

fn check_is_btree_map<K, V>(_: &BTreeMap<K, V>) {}

impl<'a> CoroFrameBuilder<'a> {
    fn compute_node_stable_indices(&mut self) -> HashMap<NodeRef, u32> {
        let mut nodes = HashMap::new();
        self._collect_nodes_in_scope(self.graph.entry, &mut nodes);
        check_is_btree_map(&self.graph.tokens);
        for (token, scope) in self.graph.tokens.iter() {
            self._collect_nodes_in_scope(*scope, &mut nodes);
        }
        nodes
    }

    fn _collect_nodes_in_scope(&mut self, scope: CoroScopeRef, nodes: &mut HashMap<NodeRef, u32>) {
        assert_eq!(self.current_scope, None);
        self.current_scope = Some(scope);
        let scope = &self.graph.get_scope(scope);
        self._collect_nodes_in_block(&scope.instructions, nodes);
        self.current_scope = None;
    }

    fn _collect_nodes_in_block(
        &self,
        block: &Vec<CoroInstrRef>,
        nodes: &mut HashMap<NodeRef, u32>,
    ) {
        for &instr_ref in block {
            let instr = self.graph.get_instr(instr_ref);
            match instr {
                CoroInstruction::Simple(node) => {
                    self._collect_nodes_in_simple(node, nodes);
                }
                CoroInstruction::ConditionStackReplay { items } => {
                    for item in items.iter() {
                        self._collect_nodes_in_simple(&item.node, nodes);
                    }
                }
                CoroInstruction::MakeFirstFlag | CoroInstruction::ClearFirstFlag(_) => {}
                CoroInstruction::SkipIfFirstFlag { body, .. } => {
                    self._collect_nodes_in_block(body, nodes);
                }
                CoroInstruction::Loop { cond, body } => {
                    if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                        self._collect_nodes_in_simple(cond, nodes);
                    } else {
                        unreachable!("unexpected loop condition");
                    }
                    self._collect_nodes_in_block(body, nodes);
                }
                CoroInstruction::If {
                    cond,
                    true_branch,
                    false_branch,
                } => {
                    if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                        self._collect_nodes_in_simple(cond, nodes);
                    } else {
                        unreachable!("unexpected if condition");
                    }
                    self._collect_nodes_in_block(true_branch, nodes);
                    self._collect_nodes_in_block(false_branch, nodes);
                }
                CoroInstruction::Switch {
                    cond,
                    cases,
                    default,
                } => {
                    if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                        self._collect_nodes_in_simple(cond, nodes);
                    } else {
                        unreachable!("unexpected switch condition");
                    }
                    for case in cases.iter() {
                        self._collect_nodes_in_block(&case.body, nodes);
                    }
                    self._collect_nodes_in_block(default, nodes);
                }
                CoroInstruction::Suspend { .. } => {}
                CoroInstruction::Terminate => {}
                _ => unreachable!("unexpected instruction in coroutine"),
            }
        }
    }

    fn _collect_nodes_in_basic_block(&self, block: &BasicBlock, nodes: &mut HashMap<NodeRef, u32>) {
        for node in block.iter() {
            self._collect_nodes_in_simple(&node, nodes);
        }
    }

    fn _collect_nodes_in_simple(&self, simple: &NodeRef, nodes: &mut HashMap<NodeRef, u32>) {
        if nodes.contains_key(&simple) {
            return;
        }
        nodes.insert(simple.clone(), nodes.len() as u32);
        match simple.get().instruction.as_ref() {
            Instruction::Local { init } => {
                self._collect_nodes_in_simple(init, nodes);
            }
            Instruction::Update { var, value } => {
                self._collect_nodes_in_simple(var, nodes);
                self._collect_nodes_in_simple(value, nodes);
            }
            Instruction::Call(_, args) => {
                for arg in args.iter() {
                    self._collect_nodes_in_simple(arg, nodes);
                }
            }
            Instruction::Phi(incomings) => {
                for incoming in incomings.iter() {
                    self._collect_nodes_in_simple(&incoming.value, nodes);
                }
            }
            Instruction::Return(value) => {
                if value.valid() {
                    self._collect_nodes_in_simple(value, nodes);
                }
            }
            Instruction::Loop { body, cond } => {
                self._collect_nodes_in_basic_block(body, nodes);
                self._collect_nodes_in_simple(cond, nodes);
            }
            Instruction::GenericLoop {
                prepare,
                body,
                update,
                cond,
            } => {
                self._collect_nodes_in_basic_block(prepare, nodes);
                self._collect_nodes_in_basic_block(body, nodes);
                self._collect_nodes_in_basic_block(update, nodes);
                self._collect_nodes_in_simple(cond, nodes);
            }
            Instruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                self._collect_nodes_in_simple(cond, nodes);
                self._collect_nodes_in_basic_block(true_branch, nodes);
                self._collect_nodes_in_basic_block(false_branch, nodes);
            }
            Instruction::Switch {
                value,
                default,
                cases,
            } => {
                self._collect_nodes_in_simple(value, nodes);
                self._collect_nodes_in_basic_block(default, nodes);
                for case in cases.iter() {
                    self._collect_nodes_in_basic_block(&case.block, nodes);
                }
            }
            Instruction::AdScope { body, .. } => {
                self._collect_nodes_in_basic_block(body, nodes);
            }
            Instruction::RayQuery {
                ray_query,
                on_triangle_hit,
                on_procedural_hit,
            } => {
                self._collect_nodes_in_simple(ray_query, nodes);
                self._collect_nodes_in_basic_block(on_triangle_hit, nodes);
                self._collect_nodes_in_basic_block(on_procedural_hit, nodes);
            }
            Instruction::Print { args, .. } => {
                for arg in args.iter() {
                    self._collect_nodes_in_simple(arg, nodes);
                }
            }
            Instruction::AdDetach(body) => {
                self._collect_nodes_in_basic_block(body, nodes);
            }
            Instruction::CoroRegister { value, .. } => {
                self._collect_nodes_in_simple(value, nodes);
            }
            _ => {}
        }
    }

    fn compute_scope_footprints(&mut self) -> HashMap<CoroScopeRef, CoroScopeFootprints> {
        let mut result = HashMap::new();
        let mut const_eval = ConstEval::new();
        result.insert(
            self.graph.entry,
            self._collect_footprints_in_scope(self.graph.entry, &mut const_eval),
        );
        for (_, scope) in self.graph.tokens.iter() {
            result.insert(
                *scope,
                self._collect_footprints_in_scope(*scope, &mut const_eval),
            );
        }
        result
    }

    fn _collect_footprints_in_scope(
        &mut self,
        scope: CoroScopeRef,
        const_eval: &mut ConstEval,
    ) -> CoroScopeFootprints {
        assert_eq!(self.current_scope, None);
        self.current_scope = Some(scope);
        let scope = self.graph.get_scope(scope);
        let mut footprints = CoroScopeFootprints::new();
        self._collect_footprints_in_block(&scope.instructions, &mut footprints, const_eval);
        self.current_scope = None;
        footprints.live_refs.trim_dynamic_access_chains();
        footprints
    }

    fn _collect_footprints_in_block(
        &self,
        block: &Vec<CoroInstrRef>,
        footprints: &mut CoroScopeFootprints,
        const_eval: &mut ConstEval,
    ) {
        for instr_ref in block {
            let instr = self.graph.get_instr(*instr_ref);
            match instr {
                CoroInstruction::Simple(node) => {
                    self._collect_footprints_in_simple(node, footprints, const_eval);
                }
                CoroInstruction::ConditionStackReplay { items } => {
                    for item in items.iter() {
                        self._collect_footprint(&item.node, footprints, const_eval);
                    }
                }
                CoroInstruction::MakeFirstFlag | CoroInstruction::ClearFirstFlag(_) => {}
                CoroInstruction::SkipIfFirstFlag { body, .. } => {
                    self._collect_footprints_in_block(body, footprints, const_eval);
                }
                CoroInstruction::Loop { cond, body } => {
                    if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                        self._collect_footprint(cond, footprints, const_eval);
                    } else {
                        unreachable!("unexpected loop condition");
                    }
                    self._collect_footprints_in_block(body, footprints, const_eval);
                }
                CoroInstruction::If {
                    cond,
                    true_branch,
                    false_branch,
                } => {
                    if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                        self._collect_footprint(cond, footprints, const_eval);
                    } else {
                        unreachable!("unexpected if condition");
                    }
                    self._collect_footprints_in_block(true_branch, footprints, const_eval);
                    self._collect_footprints_in_block(false_branch, footprints, const_eval);
                }
                CoroInstruction::Switch {
                    cond,
                    default,
                    cases,
                } => {
                    if let CoroInstruction::Simple(value) = self.graph.get_instr(*cond) {
                        self._collect_footprint(value, footprints, const_eval);
                    } else {
                        unreachable!("unexpected switch condition");
                    }
                    for case in cases.iter() {
                        self._collect_footprints_in_block(&case.body, footprints, const_eval);
                    }
                    self._collect_footprints_in_block(default, footprints, const_eval);
                }
                CoroInstruction::Suspend { .. } => {}
                CoroInstruction::Terminate => {}
                _ => unreachable!("unexpected instruction in coroutine"),
            }
        }
    }

    fn _collect_footprints_in_basic_block(
        &self,
        block: &BasicBlock,
        footprints: &mut CoroScopeFootprints,
        const_eval: &mut ConstEval,
    ) {
        for node in block.iter() {
            self._collect_footprints_in_simple(&node, footprints, const_eval);
        }
    }

    fn _collect_footprint(
        &self,
        node: &NodeRef,
        footprints: &mut CoroScopeFootprints,
        const_eval: &mut ConstEval,
    ) {
        let (root, chain) = AccessTree::access_chain_from_gep_chain(node.clone());
        let chain = AccessTree::partially_evaluate_access_chain(chain.as_slice(), const_eval);
        let current_scope = self.current_scope.unwrap();
        let live_states = &self.transfer_graph.nodes[&current_scope].union_live_states;
        if live_states.maybe_overlaps(root, chain.as_slice()) {
            footprints.live_refs.insert(root, chain.as_slice());
        }
    }

    fn _collect_footprints_in_simple(
        &self,
        simple: &NodeRef,
        footprints: &mut CoroScopeFootprints,
        const_eval: &mut ConstEval,
    ) {
        self._collect_footprint(simple, footprints, const_eval);
        match simple.get().instruction.as_ref() {
            Instruction::Local { init } => {
                footprints.locals.insert(simple.clone());
                self._collect_footprint(init, footprints, const_eval);
            }
            Instruction::Update { var, value } => {
                self._collect_footprint(var, footprints, const_eval);
                self._collect_footprint(value, footprints, const_eval);
            }
            Instruction::Call(_, args) => {
                for arg in args.iter() {
                    self._collect_footprint(arg, footprints, const_eval);
                }
            }
            Instruction::Phi(incomings) => {
                for incoming in incomings.iter() {
                    self._collect_footprint(&incoming.value, footprints, const_eval);
                }
            }
            Instruction::Return(value) => {
                if value.valid() {
                    self._collect_footprint(value, footprints, const_eval);
                }
            }
            Instruction::Loop { body, cond } => {
                self._collect_footprints_in_basic_block(body, footprints, const_eval);
                self._collect_footprint(cond, footprints, const_eval);
            }
            Instruction::GenericLoop {
                prepare,
                body,
                update,
                cond,
            } => {
                self._collect_footprints_in_basic_block(prepare, footprints, const_eval);
                self._collect_footprints_in_basic_block(body, footprints, const_eval);
                self._collect_footprints_in_basic_block(update, footprints, const_eval);
                self._collect_footprint(cond, footprints, const_eval);
            }
            Instruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                self._collect_footprint(cond, footprints, const_eval);
                self._collect_footprints_in_basic_block(true_branch, footprints, const_eval);
                self._collect_footprints_in_basic_block(false_branch, footprints, const_eval);
            }
            Instruction::Switch {
                value,
                default,
                cases,
            } => {
                self._collect_footprint(value, footprints, const_eval);
                self._collect_footprints_in_basic_block(default, footprints, const_eval);
                for case in cases.iter() {
                    self._collect_footprints_in_basic_block(&case.block, footprints, const_eval);
                }
            }
            Instruction::AdScope { body, .. } => {
                self._collect_footprints_in_basic_block(body, footprints, const_eval);
            }
            Instruction::RayQuery {
                ray_query,
                on_triangle_hit,
                on_procedural_hit,
            } => {
                self._collect_footprint(ray_query, footprints, const_eval);
                self._collect_footprints_in_basic_block(on_triangle_hit, footprints, const_eval);
                self._collect_footprints_in_basic_block(on_procedural_hit, footprints, const_eval);
            }
            Instruction::Print { args, .. } => {
                for arg in args.iter() {
                    self._collect_footprint(arg, footprints, const_eval);
                }
            }
            Instruction::AdDetach(body) => {
                self._collect_footprints_in_basic_block(body, footprints, const_eval);
            }
            Instruction::CoroRegister { value, .. } => {
                self._collect_footprint(value, footprints, const_eval);
            }
            _ => {}
        }
    }
}

impl<'a> CoroFrameBuilder<'a> {
    fn build(graph: &'a CoroGraph, transfer_graph: &'a CoroTransferGraph) -> CoroFrame {
        let mut graph = CoroFrameBuilder {
            graph,
            transfer_graph,
            node_stable_indices: HashMap::new(),
            scope_footprints: HashMap::new(),
            current_scope: None,
        };
        graph.node_stable_indices = graph.compute_node_stable_indices();
        graph.scope_footprints = graph.compute_scope_footprints();
        todo!("layout coroutine frame")
    }
}

impl CoroFrame {
    pub fn build(graph: &CoroGraph, transfer_graph: &CoroTransferGraph) -> CoroFrame {
        CoroFrameBuilder::build(graph, transfer_graph)
    }
}
