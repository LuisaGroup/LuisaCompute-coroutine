// This file implements the coroutine frame analysis, which computes the layout of the frame structure
// and determine which fields should be loaded and saved for each subroutine.
// We start by computing some auxiliary sets for each subroutine:
// - ExternalUse: the set of nodes that are used inside the subroutine but defined outside (from CoroUseDef)
// - LiveOut: the set of nodes that are live at each suspension point (from CoroTransferGraph)
// - Kill: the set of nodes that are killed (definitely overwritten) at each suspension point (from CoroUseDef)
// - Touch: the set of nodes that are touched (possibly overwritten) at each suspension point (from CoroUseDef)
// - Load: the set of nodes that are loaded at the beginning of the subroutine (computed as detailed below)
// - Save: the set of nodes that are saved to the frame at each suspension point (computed as detailed below)
// To compute Load and Save (& denotes set intersection, `+` for union and `-` for subtraction):
// - Load = ((LiveOut - Kill) & Touch) + ExternalUse
// - Save = LiveOut & Touch (note: Kill is a subset of Touch)

use crate::analysis::coro_graph::{CoroGraph, CoroInstrRef, CoroInstruction, CoroScopeRef};
use crate::analysis::coro_transfer_graph::CoroTransferGraph;
use crate::ir::{BasicBlock, Instruction, NodeRef};
use std::collections::{BTreeMap, HashMap};

pub(crate) struct CoroFrame {}

struct CoroFrameBuilder<'a> {
    graph: &'a CoroGraph,
    transfer_graph: &'a CoroTransferGraph,
    node_stable_indices: HashMap<NodeRef, u32>,
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
        let scope = &self.graph.get_scope(scope);
        self._collect_nodes_in_block(&scope.instructions, nodes);
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
}

impl<'a> CoroFrameBuilder<'a> {
    fn build(graph: &'a CoroGraph, transfer_graph: &'a CoroTransferGraph) -> CoroFrame {
        let mut graph = CoroFrameBuilder {
            graph,
            transfer_graph,
            node_stable_indices: HashMap::new(),
        };
        graph.node_stable_indices = graph.compute_node_stable_indices();
        todo!("layout coroutine frame")
    }
}

impl CoroFrame {
    pub fn build(graph: &CoroGraph, transfer_graph: &CoroTransferGraph) -> CoroFrame {
        CoroFrameBuilder::build(graph, transfer_graph)
    }
}
