// This file implements the coroutine transfer graph analysis, which records the
// topology of a coroutine and computes the alive states at each suspension point.
// The result is used in the coroutine frame layout analysis and will be passed to
// the frontend to help the scheduler to optimize the coroutine execution.

use crate::analysis::coro_graph::{CoroGraph, CoroInstrRef, CoroInstruction, CoroScopeRef};
use crate::analysis::coro_use_def::{AccessTree, CoroGraphDefUse};
use std::collections::HashMap;

struct CoroTransferEdge {
    // the target of the transfer (another subscope)
    target: CoroScopeRef,
    // the alive states at the suspension point, i.e. the states that will be read
    // from reachable subscopes and should thus be saved in the coroutine frame
    alive_states: AccessTree,
}

// A subscope of the coroutine
struct CoroTransferNode {
    scope: CoroScopeRef,
    outlets: Vec<CoroTransferEdge>,
    union_alive_states: AccessTree,
}

pub(crate) struct CoroTransferGraph {
    nodes: HashMap<CoroScopeRef, CoroTransferNode>,
}

impl CoroTransferGraph {
    pub fn dump(&self) {
        println!("=========================== CoroTransferGraph ===========================");
        for i in 0..self.nodes.len() {
            let scope = CoroScopeRef(i);
            let node = &self.nodes[&scope];
            println!(
                "======================== Subscope {} (Outlets: {}) ========================",
                i,
                node.outlets.len()
            );
            for edge in node.outlets.iter() {
                println!("- Target: {} -", edge.target.0);
                edge.alive_states.dump();
            }
            println!("- Union Alive States -");
            node.union_alive_states.dump();
        }
    }
}

struct CoroTransferGraphBuilder<'a> {
    graph: &'a CoroGraph,
    use_def: &'a CoroGraphDefUse,
}

impl<'a> CoroTransferGraphBuilder<'a> {
    fn new(graph: &'a CoroGraph, use_def: &'a CoroGraphDefUse) -> Self {
        Self { graph, use_def }
    }

    fn probe_suspend_tokens(&self, block: &Vec<CoroInstrRef>, sp: &mut Vec<u32>) {
        for &instr in block {
            match self.graph.get_instr(instr) {
                CoroInstruction::Simple(_)
                | CoroInstruction::ConditionStackReplay { .. }
                | CoroInstruction::MakeFirstFlag
                | CoroInstruction::ClearFirstFlag(_)
                | CoroInstruction::Terminate => {}
                CoroInstruction::SkipIfFirstFlag { body, .. } => {
                    self.probe_suspend_tokens(body, sp);
                }
                CoroInstruction::Loop { body, .. } => {
                    self.probe_suspend_tokens(body, sp);
                }
                CoroInstruction::If {
                    true_branch,
                    false_branch,
                    ..
                } => {
                    self.probe_suspend_tokens(true_branch, sp);
                    self.probe_suspend_tokens(false_branch, sp);
                }
                CoroInstruction::Switch { cases, default, .. } => {
                    for case in cases.iter() {
                        self.probe_suspend_tokens(&case.body, sp);
                    }
                    self.probe_suspend_tokens(default, sp);
                }
                CoroInstruction::Suspend { token } => {
                    sp.push(*token);
                }
                _ => unreachable!("unexpected instruction in coroutine"),
            }
        }
    }

    fn build_transfer_topology(&self, g: &mut CoroTransferGraph) {
        for (i, scope) in self.graph.scopes.iter().enumerate() {
            // probe the suspension points in the subscope
            let mut suspend_tokens = Vec::new();
            self.probe_suspend_tokens(&scope.instructions, &mut suspend_tokens);
            // sort and deduplicate
            suspend_tokens.sort_unstable();
            suspend_tokens.dedup();
            // create a node for the subscope
            let scope_ref = CoroScopeRef(i);
            let node = CoroTransferNode {
                scope: scope_ref,
                outlets: suspend_tokens
                    .iter()
                    .map(|t| {
                        let target = self.graph.tokens[t];
                        CoroTransferEdge {
                            target,
                            alive_states: self.use_def.scopes[&target].external_uses.clone(),
                        }
                    })
                    .collect(),
                union_alive_states: AccessTree::new(),
            };
            // insert the node into the graph
            g.nodes.insert(scope_ref, node);
        }
    }

    fn compute_alive_states(&self, g: &mut CoroTransferGraph) {
        let mut any_change = true;
        while any_change {
            any_change = false;
            let gg = unsafe { &*(g as *mut CoroTransferGraph) };
            for node in g.nodes.values_mut() {
                for edge in node.outlets.iter_mut() {
                    // propagate the alive states from the target subscopes
                    let new_alive_states = gg.nodes[&edge.target]
                        .outlets
                        .iter()
                        .fold(edge.alive_states.clone(), |acc, e| {
                            acc.union(&e.alive_states)
                        });
                    if new_alive_states != edge.alive_states {
                        any_change = true;
                        edge.alive_states = new_alive_states;
                    }
                }
            }
        }
        for node in g.nodes.values_mut() {
            // compute the union of the alive states at all the suspension points
            node.union_alive_states = node
                .outlets
                .iter()
                .fold(AccessTree::new(), |acc, e| acc.union(&e.alive_states));
        }
    }

    fn build(&self) -> CoroTransferGraph {
        let mut g = CoroTransferGraph {
            nodes: HashMap::new(),
        };
        self.build_transfer_topology(&mut g);
        self.compute_alive_states(&mut g);
        g
    }
}

impl CoroTransferGraph {
    pub fn build(graph: &CoroGraph, use_def: &CoroGraphDefUse) -> Self {
        CoroTransferGraphBuilder::new(graph, use_def).build()
    }
}
