// This file implements the coroutine transition graph analysis, which records the
// topology of a coroutine and computes the alive states at each suspension point.
// The result is used in the coroutine frame layout analysis and will be passed to
// the frontend to help the scheduler to optimize the coroutine execution.
// The analysis computes the following auxiliary sets for each subscope (`&` denotes
// set intersection, `+` for union and `-` for subtraction):z
// - Live: the set of nodes that are live at each suspension point
//   (computed by graph-level liveness analysis)
// - Load: the set of nodes that are loaded at the beginning of the subroutine
//   (computed as [((Live - InternalKill) & InternalTouch) + ExternalUse])
// - Save: the set of nodes that are saved to the frame at each suspension point
//   (computed as [Live & Touch])

use crate::analysis::coro_graph::{CoroGraph, CoroInstrRef, CoroInstruction, CoroScopeRef};
use crate::analysis::coro_use_def::CoroGraphUseDef;
use crate::analysis::utility::AccessTree;
use crate::safe;
use std::collections::{BTreeMap, HashMap};

pub(crate) struct CoroTransitionEdge {
    // the target of the transition (another subscope)
    pub target: CoroScopeRef,
    // the live states at the suspension point, i.e. the states that will be read
    // from reachable subscopes and should thus be saved in the coroutine frame
    pub live_states: AccessTree,
    pub states_to_load: AccessTree,
    pub states_to_save: AccessTree,
}

// A subscope of the coroutine
pub(crate) struct CoroTransitionState {
    pub scope: CoroScopeRef,
    pub outlets: BTreeMap<u32, CoroTransitionEdge>,
    pub union_live_states: AccessTree,
    pub union_states_to_load: AccessTree,
    pub union_states_to_save: AccessTree,
}

pub(crate) struct CoroTransitionGraph {
    pub union_states: AccessTree,
    pub nodes: HashMap<CoroScopeRef, CoroTransitionState>,
}

impl CoroTransitionGraph {
    pub fn dump(&self) {
        println!("=========================== CoroTransitionGraph ===========================");
        for i in 0..self.nodes.len() {
            let scope = CoroScopeRef(i);
            let node = &self.nodes[&scope];
            println!(
                "======================== Subscope {} (Outlets: {}) ========================",
                i,
                node.outlets.len()
            );
            for (_, edge) in node.outlets.iter() {
                println!("- Target {} Live States -", edge.target.0);
                edge.live_states.dump();
            }
            println!("- Union Live States -");
            node.union_live_states.dump();
            println!("- States to Load -");
            node.union_states_to_load.dump();
            println!("- States to Save -");
            node.union_states_to_save.dump();
        }
    }
}

struct CoroTransitionGraphBuilder<'a> {
    graph: &'a CoroGraph,
    use_def: &'a CoroGraphUseDef,
}

impl<'a> CoroTransitionGraphBuilder<'a> {
    fn new(graph: &'a CoroGraph, use_def: &'a CoroGraphUseDef) -> Self {
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

    fn build_transition_topology(&self, g: &mut CoroTransitionGraph) {
        for (i, scope) in self.graph.scopes.iter().enumerate() {
            // probe the suspension points in the subscope
            let mut suspend_tokens = Vec::new();
            self.probe_suspend_tokens(&scope.instructions, &mut suspend_tokens);
            // sort and deduplicate
            suspend_tokens.sort_unstable();
            suspend_tokens.dedup();
            // create a node for the subscope
            let scope_ref = CoroScopeRef(i);
            let node = CoroTransitionState {
                scope: scope_ref,
                outlets: suspend_tokens
                    .iter()
                    .map(|t| {
                        let target = self.graph.tokens[t];
                        let edge = CoroTransitionEdge {
                            target,
                            live_states: self.use_def.scopes[&target].external_uses.clone(),
                            states_to_load: AccessTree::new(),
                            states_to_save: AccessTree::new(),
                        };
                        (*t, edge)
                    })
                    .collect(),
                union_live_states: AccessTree::new(),
                union_states_to_load: AccessTree::new(),
                union_states_to_save: AccessTree::new(),
            };
            // insert the node into the graph
            g.nodes.insert(scope_ref, node);
        }
    }

    fn compute_live_states(&self, g: &mut CoroTransitionGraph) {
        let mut any_change = true;
        while any_change {
            any_change = false;
            let gg = safe! { &*(g as *mut CoroTransitionGraph) };
            for node in g.nodes.values_mut() {
                for (_, edge) in node.outlets.iter_mut() {
                    // propagate the alive states from the target subscopes
                    let target_use_def = &self.use_def.scopes[&edge.target];
                    let target_outlets = &gg.nodes[&edge.target].outlets;
                    let mut new_live_states =
                        target_outlets
                            .iter()
                            .fold(edge.live_states.clone(), |acc, (_, e)| {
                                let killed = &target_use_def.internal_kills[&e.target];
                                acc.union(&e.live_states.subtract(killed))
                            });
                    // special: add also the designated values, they may be used anywhere externally
                    for (_, &designated) in &self.graph.designated_values {
                        new_live_states.insert(designated, &[]);
                    }
                    // check if fixed point is reached
                    if new_live_states != edge.live_states {
                        any_change = true;
                        edge.live_states = new_live_states;
                    }
                }
            }
        }
        for node in g.nodes.values_mut() {
            // compute the union of the alive states at all the suspension points
            node.union_live_states = node
                .outlets
                .iter()
                .fold(AccessTree::new(), |acc, (_, e)| acc.union(&e.live_states));
            let external_use = &self.use_def.scopes[&node.scope].external_uses;
            for (_, edge) in node.outlets.iter_mut() {
                let live = &edge.live_states;
                // TODO: use per-target touch sets
                let touch = &self.use_def.scopes[&node.scope].internal_touches;
                let internal_kill = &self.use_def.scopes[&node.scope].internal_kills[&edge.target];
                // Load = ((Live - InternalKill) & Touch) + ExternalUses
                edge.states_to_load = AccessTree::union(
                    &AccessTree::intersect(&AccessTree::subtract(live, internal_kill), touch),
                    external_use,
                );
                // Save = Live & Touch
                edge.states_to_save = AccessTree::intersect(live, touch);
            }
            node.union_states_to_load = node
                .outlets
                .iter()
                .fold(external_use.clone(), |acc, (_, e)| {
                    acc.union(&e.states_to_load)
                });
            node.union_states_to_save =
                node.outlets.iter().fold(AccessTree::new(), |acc, (_, e)| {
                    acc.union(&e.states_to_save)
                });
        }
    }

    fn build(&self) -> CoroTransitionGraph {
        let mut g = CoroTransitionGraph {
            union_states: self.use_def.union_uses.clone(),
            nodes: HashMap::new(),
        };
        self.build_transition_topology(&mut g);
        self.compute_live_states(&mut g);
        g
    }
}

impl CoroTransitionGraph {
    pub fn build(graph: &CoroGraph, use_def: &CoroGraphUseDef) -> Self {
        CoroTransitionGraphBuilder::new(graph, use_def).build()
    }
}
