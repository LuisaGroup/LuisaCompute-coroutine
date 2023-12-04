// This file implements the coroutine graph extraction analysis, which traverse the IR to
// find all coroutine split marks and store them into a graph. Each graph node records the
// corresponding instructions that the subroutine contains. A subroutine is a sequence of
// instructions between two coroutine split marks, i.e., instructions from one split mark
// through all the reachable instructions until the next split mark is encountered.
// Note: this analysis only works on a single module without recurse into its callees.

use crate::ir::{BasicBlock, Func, Instruction, Module, NodeRef};
use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct CoroInstrRef(usize);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct CoroScopeRef(usize);

impl CoroInstrRef {
    fn invalid() -> Self {
        Self(usize::MAX)
    }
}

impl CoroScopeRef {
    fn invalid() -> Self {
        Self(usize::MAX)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ConditionStackItem {
    // the condition node (for `if` and `switch`)
    pub node: NodeRef,
    // the condition value that leads to the current instruction
    // (0/1 for `if`, i32 values for `switch` cases, `loops` and
    // `switch` default are not recorded)
    pub value: i32,
}

#[derive(Clone)]
pub(crate) struct CoroSwitchCase {
    pub value: i32,
    pub body: Vec<CoroInstrRef>, // indices into the graph nodes
}

#[derive(Clone)]
pub(crate) enum CoroInstruction {
    // entry
    Entry,

    // entry scope
    EntryScope {
        body: Vec<CoroInstrRef>, // indices into the graph nodes
    },

    // A simple IR node that does not contain any nested basic blocks.
    Simple(NodeRef),

    // replay the condition stack
    ConditionStackReplay {
        items: Vec<ConditionStackItem>,
    },

    MakeFirstFlag, // initialization of a first flag
    SkipIfFirstFlag {
        flag: CoroInstrRef,      // indices into the graph nodes
        body: Vec<CoroInstrRef>, // indices into the graph nodes
    },
    ClearFirstFlag(CoroInstrRef), // clear a first flag

    // A do-while loop
    Loop {
        body: Vec<CoroInstrRef>, // indices into the graph nodes
        cond: CoroInstrRef,      // indices into the graph nodes
    },

    // An if-then-else branch
    If {
        cond: CoroInstrRef,              // indices into the graph nodes
        true_branch: Vec<CoroInstrRef>,  // indices into the graph nodes
        false_branch: Vec<CoroInstrRef>, // indices into the graph nodes
    },

    // A switch statement
    Switch {
        cond: CoroInstrRef,         // indices into the graph nodes
        cases: Vec<CoroSwitchCase>, // indices into the graph nodes
        default: Vec<CoroInstrRef>, // indices into the graph nodes
    },

    // An suspend mark
    Suspend {
        token: u32,
    },

    // A terminate mark
    Terminate,
}

pub(crate) struct CoroScope {
    pub instructions: Vec<CoroInstrRef>, // indices into the graph nodes
}

// This struct is a direct translation from the IR to the coroutine graph without
// splitting the coroutine scopes, i.e., the only scope is the root (entry) scope.
pub(crate) struct CoroPreliminaryGraph {
    pub entry_scope: CoroInstrRef,          // index of the entry scope
    pub instructions: Vec<CoroInstruction>, // all the instructions in the graph
    pub node_to_instr: HashMap<NodeRef, CoroInstrRef>, // IR node to instruction index
}

impl CoroPreliminaryGraph {
    fn translate_node(
        node: &NodeRef,
        instructions: &mut Vec<CoroInstruction>,
        node_to_instr: &mut HashMap<NodeRef, CoroInstrRef>,
    ) -> CoroInstrRef {
        assert!(!node_to_instr.contains_key(node), "Duplicate node in IR.");
        macro_rules! register {
            ($node: expr, $instr: expr) => {{
                let index = CoroInstrRef(instructions.len());
                instructions.push($instr);
                node_to_instr.insert($node.clone(), index);
                assert_eq!(instructions.len(), node_to_instr.len());
                index
            }};
        }
        match node.get().instruction.as_ref() {
            Instruction::Return(_) => register!(node, CoroInstruction::Terminate),
            Instruction::Loop { body, cond } => {
                let body = Self::translate_block(body, instructions, node_to_instr);
                let cond = node_to_instr.get(cond).unwrap().clone();
                let instr = CoroInstruction::Loop { body, cond };
                register!(node, instr)
            }
            Instruction::GenericLoop { .. } => panic!("Unexpected GenericLoop instruction."),
            Instruction::Break => panic!("Unexpected Break instruction."),
            Instruction::Continue => panic!("Unexpected Continue instruction."),
            Instruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                let cond = node_to_instr.get(cond).unwrap().clone();
                let true_branch = Self::translate_block(true_branch, instructions, node_to_instr);
                let false_branch = Self::translate_block(false_branch, instructions, node_to_instr);
                let instr = CoroInstruction::If {
                    cond,
                    true_branch,
                    false_branch,
                };
                register!(node, instr)
            }
            Instruction::Switch {
                value,
                cases,
                default,
            } => {
                let value = node_to_instr.get(value).unwrap().clone();
                let cases = cases
                    .iter()
                    .map(|case| CoroSwitchCase {
                        value: case.value,
                        body: Self::translate_block(&case.block, instructions, node_to_instr),
                    })
                    .collect();
                let default = Self::translate_block(default, instructions, node_to_instr);
                let instr = CoroInstruction::Switch {
                    cond: value,
                    cases,
                    default,
                };
                register!(node, instr)
            }
            Instruction::CoroSplitMark { token } => {
                register!(node, CoroInstruction::Suspend { token: *token })
            }
            Instruction::CoroSuspend { .. } => panic!("Unexpected CoroSuspend instruction."),
            Instruction::CoroResume { .. } => panic!("Unexpected CoroResume instruction."),
            _ => register!(node, CoroInstruction::Simple(node.clone())),
        }
    }

    fn translate_block(
        block: &BasicBlock,
        instructions: &mut Vec<CoroInstruction>,
        node_to_instr: &mut HashMap<NodeRef, CoroInstrRef>,
    ) -> Vec<CoroInstrRef> {
        block
            .iter()
            .map(|node| Self::translate_node(&node, instructions, node_to_instr))
            .collect()
    }

    pub fn from(module: &Module) -> Self {
        let mut instructions = Vec::new();
        let mut node_to_instr = HashMap::new();
        let mut entry_scope =
            Self::translate_block(module.entry.as_ref(), &mut instructions, &mut node_to_instr);
        // add the entry scope to the graph
        let entry_instr = CoroInstruction::Entry;
        let entry = CoroInstrRef(instructions.len());
        instructions.push(entry_instr);
        entry_scope.insert(0, entry);
        let entry_scope = CoroInstruction::EntryScope { body: entry_scope };
        let entry = CoroInstrRef(instructions.len());
        instructions.push(entry_scope);
        Self {
            entry_scope: entry,
            instructions,
            node_to_instr,
        }
    }
}

// This struct is the final coroutine graph after splitting the coroutine scopes.
pub(crate) struct CoroGraph {
    scopes: Vec<CoroScope>,                        // all the scopes in the graph
    entry: CoroScopeRef,                           // the index of the entry scope (the root scope)
    marks: HashMap<u32, CoroScopeRef>,             // map from split mark token to scope index
    instructions: Vec<CoroInstruction>,            // all the instructions in the graph
    node_to_instr: HashMap<NodeRef, CoroInstrRef>, // IR node to instruction index
}

// Method:
// Auxiliary data structures:
//   1. Instruction stack S: a stack of the parent control flow instructions containing the current
//      instruction for convenient backtracking and dominance checking (details below).
//   2. Condition stack C: a stack of the condition values (`true` or `false`) of the parent control
//      flow that leads to the current instruction for convenient replay.
// Continuation extraction: when a split mark is encountered, create a new scope and traverse the IR
// until another split mark is encountered. Details to process the control flows in the continuation:
//   1. All instructions dominated by the split mark are added to the start of the new sub-coroutine:
//      a. If S[:-1] (i.e., the instruction stack without the *direct* parent) contain loops, then
//         only the remaining instructions in the current block are dominated by the split mark.
//      b. If S[:-1] are non-loops (`if`s and `switch`es), then all reachable instructions from the
//         split mark are dominated by the split mark (as we will never loop back to the precedents).
//   2. For the non-dominated instructions, the control flows are copied into the new sub-coroutine.
//      However, we need to maintain a `first` flag that helps skip the instructions when we replay
//      the condition stack. The `first` flag is assigned to `false` at the original split mark.
//      - Remark 1: we replay the condition stack to "fool" the following def-use analysis, so that we
//        can avoid passing conditions through the coroutine frame. Maybe we can replay more information
//        in the future.
//      - Remark 2: Special handling when the only direct parent is a `loop` (i.e., S[-1] is a `loop`
//        and S[:-1] is empty): the loop can be simplified to an `if`. (Otherwise we may also have it
//        done in a later `simplify_control_flow` pass.)
//   4. Each new sub-coroutine ends with another split mark or a terminate mark (end of the function).
//      So the split-into-continuation process is recursive.
//
// Example #1 (simple `if`):
//   if (cond) {
//       << A >>
//       suspend(token); // C = [cond = true]
//       << B >>
//   } else {
//      << C >>
//   }
//   << D >>
// Continuation from the `suspend` instruction:
//   cond = true;// condition stack replay
//   << B >>
//   << D >>
//
// Example #2 (the only, direct parent is a `loop`):
//   do {
//       << A >>
//       suspend(token);
//       << B >>
//   } while (cond);
//   << C >>
// Continuation from the `suspend` instruction (note the special handling in [Step 2. Remark 2.] above):
//   << B >>
//   if (cond) {
//       << A >>
//       suspend(token);
//   }
//   << C >>
//
// Example #3 (`loop` mixed with `if`):
//   do {
//       << A >>
//       if (cond1) {
//           << B >>
//           suspend(token); // C = [cond1 = true]
//           << C >>
//       } else {
//           << D >>
//       }
//       << E >>
//   } while (cond2);
//   << F >>
// Continuation from the `suspend` instruction:
//   cond1 = true;// condition stack replay
//   << C >>      // only the remaining instructions in the current block are dominated by the split mark
//   first = true;// first flag
//   do {
//       // mask the instructions in the loop body that precede the split mark with the `first` flag
//       if (!first) {
//           << A >>
//       }
//       if (cond1) {
//           if (!first) {
//               << B >>
//               suspend(token);
//           }
//           first = false; // reset the `first` flag at the original split mark
//       } else {
//           << D >>
//       }
//       // instructions after the split mark are executed normally
//       << E >>
//   } while (cond2);
//   << F >>
//
// Example #4 (nested `loop`s):
//   do {// outer loop
//       << A >>
//       do {// inner loop
//           << B >>
//           suspend(token);
//           << C >>
//       } while (cond1);
//       << D >>
//   } while (cond2);
//   << E >>
// Continuation from the `suspend` instruction:
//   << C >>      // only the remaining instructions in the current block are dominated by the split mark
//   first = true;// first flag
//   do {// outer loop
//       // mask the instructions in the loop body that precede the split mark with the `first` flag
//       if (!first) {
//           << A >>
//       }
//       do {// inner loop
//           // mask the instructions in the loop body that precede the split mark with the `first` flag
//           if (!first) {
//               << B >>
//               suspend(token);
//           }
//           first = false; // reset the `first` flag at the original split mark
//       } while (cond1);
//       // instructions after the split mark are executed normally
//       << D >>
//   } while (cond2);
//   << E >>
// With the special handling in [Step 2. Remark 2.] above, the inner loop can be simplified to an `if`:
//   << C >>
//   first = true;
//   do {// outer loop
//       // mask the instructions in the loop body that precede the split mark with the `first` flag
//       if (!first) {
//           << A >>
//       }
//       if (cond1) {
//           << B >>
//           suspend(token);
//       }
//       first = false; // reset the `first` flag at the original split mark
//       // instructions after the split mark are executed normally
//       << D >>
//   } while (cond2);
//   << E >>
//
// Example #5 (nested `loop`s and `if`s):
//   do {// outer loop
//       << A >>
//       do {// inner loop
//           << B >>
//           if (cond1) {
//               << C >>
//               suspend(token);// C = [cond1 = true]
//               << D >>
//           } else {
//               << E >>
//           }
//           << F >>
//       } while (cond2);
//       << G >>
//   } while (cond3);
//   << H >>
// Continuation from the `suspend` instruction:
//   cond1 = true;// condition stack replay
//   << D >>      // only the remaining instructions in the current block are dominated by the split mark
//   first = true;// first flag
//   do {// outer loop
//       // mask the instructions in the loop body that precede the split mark with the `first` flag
//       if (!first) {
//          << A >>
//       }
//       do {// inner loop
//           // mask the instructions in the loop body that precede the split mark with the `first` flag
//           if (!first) {
//               << B >>
//           }
//           if (cond1) {
//               if (!first) {
//                   << C >>
//                   suspend(token);
//               }
//               first = false; // reset the `first` flag at the original split mark
//           } else {
//               << E >>
//           }
//           // instructions after the split mark are executed normally
//           << F >>
//       } while (cond2);
//       // instructions after the split mark are executed normally
//       << G >>
//   } while (cond3);
//   << H >>
//
// Example #6 (nested `loop`s and `if`s):
//   do {// outer loop
//      << A >>
//      if (cond1) {
//          << B >>
//          do {// inner loop
//              << C >>
//              suspend(token);// C = [cond1 = true]
//              << D >>
//          } while (cond2);
//          << E >>
//      } else {
//          << F >>
//      }
//      << G >>
//   } while (cond3);
//   << H >>
// Continuation from the `suspend` instruction:
//   cond1 = true;// condition stack replay
//   << D >>      // only the remaining instructions in the current block are dominated by the split mark
//   first = true;// first flag
//   do {// outer loop
//       // mask the instructions in the loop body that precede the split mark with the `first` flag
//       if (!first) {
//           << A >>
//       }
//       if (cond1) {
//           if (!first) {
//               << B >>
//           }
//           do {// inner loop
//               // mask the instructions in the loop body that precede the split mark with the `first` flag
//               if (!first) {
//                   << C >>
//                   suspend(token);
//               }
//               first = false; // reset the `first` flag at the original split mark
//               // instructions after the split mark are executed normally
//               << D >>
//           } while (cond2);
//           // instructions after the split mark are executed normally
//           << E >>
//       } else {
//           << F >>
//       }
//       // instructions after the split mark are executed normally
//       << G >>
//   } while (cond3);
//   << H >>

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct CoroGraphIndexer {
    parent: CoroInstrRef,
    parent_branch: usize,
    index_in_parent_branch: usize,
}

impl CoroGraph {
    pub(crate) fn instr(&self, i: CoroInstrRef) -> &CoroInstruction {
        &self.instructions[i.0]
    }

    pub(crate) fn get(&self, i: CoroGraphIndexer) -> CoroInstrRef {
        let parent_instr = self.instr(i.parent);
        match parent_instr {
            CoroInstruction::EntryScope { body } => {
                assert_eq!(i.parent_branch, 0);
                assert!(i.index_in_parent_branch < body.len());
                body[i.parent_branch]
            }
            CoroInstruction::Loop { body, .. } => {
                assert_eq!(i.parent_branch, 0);
                assert!(i.index_in_parent_branch < body.len());
                body[i.parent_branch]
            }
            CoroInstruction::If {
                true_branch,
                false_branch,
                ..
            } => match i.parent_branch {
                0 => {
                    assert!(i.index_in_parent_branch < true_branch.len());
                    true_branch[i.index_in_parent_branch]
                }
                1 => {
                    assert!(i.index_in_parent_branch < false_branch.len());
                    false_branch[i.index_in_parent_branch]
                }
                _ => panic!("Unexpected branch index."),
            },
            CoroInstruction::Switch { cases, default, .. } => {
                if i.parent_branch < cases.len() {
                    assert!(i.index_in_parent_branch < cases[i.parent_branch].body.len());
                    cases[i.parent_branch].body[i.index_in_parent_branch]
                } else {
                    assert_eq!(i.parent_branch, cases.len());
                    assert!(i.index_in_parent_branch < default.len());
                    default[i.index_in_parent_branch]
                }
            }
            _ => panic!("Not a control-flow instruction."),
        }
    }

    pub(crate) fn get_instr(&self, i: CoroGraphIndexer) -> &CoroInstruction {
        self.instr(self.get(i))
    }

    fn add(&mut self, instr: CoroInstruction) -> CoroInstrRef {
        let i = CoroInstrRef(self.instructions.len());
        self.instructions.push(instr);
        i
    }
}

impl CoroGraph {
    fn replay_condition_stack(
        graph: &mut CoroGraph,
        ancestors: &Vec<CoroGraphIndexer>,
        subscope: &mut CoroScope,
    ) {
        let stack: Vec<_> = ancestors
            .iter()
            .filter_map(|ancestor| {
                let instr = graph.instr(ancestor.parent);
                match instr {
                    CoroInstruction::If { cond, .. } => {
                        if let CoroInstruction::Simple(node) = graph.instr(*cond) {
                            let value = match ancestor.parent_branch {
                                0 => true as i32,
                                1 => false as i32,
                                _ => panic!("Unexpected branch index."),
                            };
                            Some(ConditionStackItem { node: *node, value })
                        } else {
                            panic!("Unexpected instruction.");
                        }
                    }
                    CoroInstruction::Switch { cond, cases, .. } => {
                        if let CoroInstruction::Simple(node) = graph.instr(*cond) {
                            if ancestor.parent_branch < cases.len() {
                                // we can determine the case value from the branch index
                                let value = cases[ancestor.parent_branch].value;
                                Some(ConditionStackItem { node: *node, value })
                            } else {
                                None // ignore the default branch as it does not ensure the condition to be constant
                            }
                        } else {
                            panic!("Unexpected instruction.");
                        }
                    }
                    _ => None,
                }
            })
            .collect();
        if !stack.is_empty() {
            let replay = graph.add(CoroInstruction::ConditionStackReplay { items: stack });
            subscope.instructions.push(replay);
        }
    }

    fn find_dominated_instructions(
        graph: &mut CoroGraph,
        current: CoroGraphIndexer,
        ancestors: &Vec<CoroGraphIndexer>,
        subscope: &mut CoroScope,
    ) {
    }

    fn construct_subscope(
        graph: &mut CoroGraph,
        current: CoroGraphIndexer,
        ancestors: &Vec<CoroGraphIndexer>,
    ) -> CoroScope {
        let mut subscope = CoroScope {
            instructions: Vec::new(),
        };
        Self::replay_condition_stack(graph, ancestors, &mut subscope);
        Self::find_dominated_instructions(graph, current, ancestors, &mut subscope);

        todo!()
    }

    fn is_terminator(
        graph: &CoroGraph,
        instr_ref: &CoroInstrRef,
        known: &mut HashMap<CoroInstrRef, bool>,
    ) -> bool {
        if let Some(&is_terminator) = known.get(instr_ref) {
            return is_terminator;
        }
        let instr = graph.instr(*instr_ref);
        let result = match instr {
            CoroInstruction::Simple(node) => match node.get().instruction.as_ref() {
                Instruction::Call(func, _) => match func {
                    Func::Unreachable(_) => true,
                    _ => false,
                },
                _ => false,
            },
            CoroInstruction::Entry
            | CoroInstruction::Suspend { .. }
            | CoroInstruction::EntryScope { .. }
            | CoroInstruction::Terminate => true,
            CoroInstruction::Loop { body, .. } => body
                .iter()
                .any(|&instr_ref| Self::is_terminator(graph, &instr_ref, known)),
            CoroInstruction::If {
                true_branch,
                false_branch,
                ..
            } => {
                true_branch
                    .iter()
                    .any(|&instr_ref| Self::is_terminator(graph, &instr_ref, known))
                    && false_branch
                        .iter()
                        .any(|&instr_ref| Self::is_terminator(graph, &instr_ref, known))
            }
            CoroInstruction::Switch { cases, default, .. } => {
                cases.iter().all(|case| {
                    case.body
                        .iter()
                        .any(|&instr_ref| Self::is_terminator(graph, &instr_ref, known))
                }) && default
                    .iter()
                    .any(|&instr_ref| Self::is_terminator(graph, &instr_ref, known))
            }
            _ => false,
        };
        known.insert(*instr_ref, result);
        result
    }

    fn terminated_in_current_branch(
        graph: &CoroGraph,
        current: CoroGraphIndexer,
        known: &mut HashMap<CoroInstrRef, bool>,
    ) -> bool {
        let parent = graph.instr(current.parent);
        let n_skip = current.index_in_parent_branch + 1;
        match parent {
            CoroInstruction::EntryScope { .. } => true,
            CoroInstruction::If {
                true_branch,
                false_branch,
                ..
            } => {
                let branch = match current.parent_branch {
                    0 => true_branch,
                    1 => false_branch,
                    _ => panic!("Unexpected branch index."),
                };
                branch
                    .iter()
                    .skip(n_skip)
                    .any(|&instr_ref| Self::is_terminator(graph, &instr_ref, known))
            }
            CoroInstruction::Switch { cases, default, .. } => {
                let branch = if current.parent_branch < cases.len() {
                    &cases[current.parent_branch].body
                } else {
                    default
                };
                branch
                    .iter()
                    .skip(n_skip)
                    .any(|&instr_ref| Self::is_terminator(graph, &instr_ref, known))
            }
            CoroInstruction::Loop { body, .. } => body
                .iter()
                .skip(n_skip)
                .any(|&instr_ref| Self::is_terminator(graph, &instr_ref, known)),
            _ => panic!("Unexpected instruction."),
        }
    }

    fn find_reachable_ancestors(
        graph: &CoroGraph,
        current: CoroGraphIndexer,
        ancestors: &Vec<CoroGraphIndexer>,
    ) -> Vec<CoroGraphIndexer> {
        let mut known_terminators = HashMap::new();
        let mut reachable_ancestors = Vec::new();
        let mut instr = current;
        for ancestor in ancestors.iter().rev() {
            // if terminated in the current branch, then only the remaining ancestors are unreachable
            if Self::terminated_in_current_branch(graph, instr, &mut known_terminators) {
                break;
            }
            // otherwise, the current ancestor is reachable, we need to check the ancestor itself
            reachable_ancestors.push(*ancestor);
            instr = *ancestor;
        }
        reachable_ancestors.reverse();
        reachable_ancestors
    }

    fn extract_continuation_at_suspend(
        graph: &mut CoroGraph,
        current: CoroGraphIndexer,
        ancestors: &Vec<CoroGraphIndexer>,
    ) {
        let instr = graph.get_instr(current);
        let token = match instr {
            CoroInstruction::Entry => None,
            CoroInstruction::Suspend { token } => Some(*token),
            _ => panic!("Unexpected instruction."),
        };
        if let Some(token) = token {
            if graph.marks.contains_key(&token) {
                // the continuation has been extracted
                return;
            }
        }
        // actually extract the continuation
        let ancestors = Self::find_reachable_ancestors(graph, current, ancestors);
        let subscope = Self::construct_subscope(graph, current, &ancestors);
        // add the sub-scope to the graph
        let subscope_index = graph.scopes.len();
        graph.scopes.push(subscope);
        if let Some(token) = token {
            graph.marks.insert(token, CoroScopeRef(subscope_index));
        } else {
            graph.entry = CoroScopeRef(subscope_index);
        }
    }

    fn recurse_continuation_extraction(
        graph: &mut CoroGraph,
        instructions: &Vec<CoroInstruction>,
        parent_scope: CoroInstrRef,
        parent_branch: usize,
        body: &Vec<CoroInstrRef>,
        ancestors: &mut Vec<CoroGraphIndexer>,
    ) {
        for (instr_index, &instr_ref) in body.iter().enumerate() {
            macro_rules! recurse {
                ($branch: expr, $block: expr) => {
                    let current = CoroGraphIndexer {
                        parent: parent_scope,
                        parent_branch: parent_branch,
                        index_in_parent_branch: instr_index,
                    };
                    ancestors.push(current);
                    Self::recurse_continuation_extraction(
                        graph,
                        instructions,
                        instr_ref,
                        $branch,
                        $block,
                        ancestors,
                    );
                    let popped = ancestors.pop();
                    assert_eq!(popped, Some(current));
                };
            }
            let instr = &instructions[instr_ref.0];
            match instr {
                CoroInstruction::Entry | CoroInstruction::Suspend { .. } => {
                    // a split mark is encountered
                    Self::extract_continuation_at_suspend(
                        graph,
                        CoroGraphIndexer {
                            parent: parent_scope,
                            parent_branch: parent_branch,
                            index_in_parent_branch: instr_index,
                        },
                        ancestors,
                    );
                }
                CoroInstruction::Simple(_) => { /* do nothing */ }
                CoroInstruction::Loop { body, .. } => {
                    recurse!(0, body);
                }
                CoroInstruction::If {
                    true_branch,
                    false_branch,
                    ..
                } => {
                    recurse!(0, true_branch);
                    recurse!(1, false_branch);
                }
                CoroInstruction::Switch { cases, default, .. } => {
                    for (case_index, case) in cases.iter().enumerate() {
                        recurse!(case_index, &case.body);
                    }
                    recurse!(cases.len(), default);
                }
                CoroInstruction::Terminate => {}
                _ => panic!("Unexpected instruction."),
            }
        }
    }

    fn build(preliminary_graph: CoroPreliminaryGraph) -> CoroGraph {
        let mut graph = CoroGraph {
            scopes: Vec::new(),
            entry: CoroScopeRef::invalid(),
            marks: HashMap::new(),
            instructions: preliminary_graph.instructions.clone(),
            node_to_instr: preliminary_graph.node_to_instr,
        };
        if let CoroInstruction::EntryScope { body: entry_body } =
            &preliminary_graph.instructions[preliminary_graph.entry_scope.0]
        {
            Self::recurse_continuation_extraction(
                &mut graph,
                &preliminary_graph.instructions,
                preliminary_graph.entry_scope,
                0,
                entry_body,
                &mut Vec::new(),
            );
        } else {
            panic!("Unexpected instruction.");
        }
        graph
    }
}
