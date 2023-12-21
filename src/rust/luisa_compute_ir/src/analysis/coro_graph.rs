// This file implements the coroutine graph extraction analysis, which traverse the IR to
// find all coroutine split marks and store them into a graph. Each graph node records the
// corresponding instructions that the subroutine contains. A subroutine is a sequence of
// instructions between two coroutine split marks, i.e., instructions from one split mark
// through all the reachable instructions until the next split mark is encountered.
// Note: this analysis only works on a single module without recurse into its callees.

use crate::ir::{BasicBlock, Func, Instruction, Module, NodeRef};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct CoroInstrRef(pub usize);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct CoroScopeRef(pub usize);

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

#[derive(Clone, Debug)]
pub(crate) struct CoroSwitchCase {
    pub value: i32,
    pub body: Vec<CoroInstrRef>, // indices into the graph nodes
}

#[derive(Clone, Debug)]
pub(crate) enum CoroInstruction {
    // entry, won't exist after this analysis
    Entry,

    // entry scope, won't exist after this analysis
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

#[derive(Debug)]
pub(crate) struct CoroScope {
    pub instructions: Vec<CoroInstrRef>, // indices into the graph nodes
}

// This struct is a direct translation from the IR to the coroutine graph without
// splitting the coroutine scopes, i.e., the only scope is the root (entry) scope.
struct CoroPreliminaryGraph {
    entry_scope: CoroInstrRef,                     // index of the entry scope
    instructions: Vec<CoroInstruction>,            // all the instructions in the graph
    node_to_instr: HashMap<NodeRef, CoroInstrRef>, // IR node to instruction index
    terminators: HashSet<CoroInstrRef>,            // all the terminators in the graph (recursively)
}

impl CoroPreliminaryGraph {
    fn translate_node(
        node: &NodeRef,
        instructions: &mut Vec<CoroInstruction>,
        node_to_instr: &mut HashMap<NodeRef, CoroInstrRef>,
    ) -> CoroInstrRef {
        if let Some(instr_ref) = node_to_instr.get(node) {
            return *instr_ref;
        }

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
                let cond = Self::translate_node(cond, instructions, node_to_instr);
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
                let cond = Self::translate_node(cond, instructions, node_to_instr);
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
                let value = Self::translate_node(value, instructions, node_to_instr);
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

    fn find_terminators(
        instructions: &Vec<CoroInstruction>,
        instr_ref: &CoroInstrRef,
        known: &mut HashMap<CoroInstrRef, bool>,
    ) {
        if known.contains_key(instr_ref) {
            return;
        }
        let instr = &instructions[instr_ref.0];
        macro_rules! traverse {
            ($body: expr) => {{
                for instr_ref in $body.iter() {
                    Self::find_terminators(instructions, instr_ref, known);
                }
                $body.iter().any(|instr_ref| *known.get(instr_ref).unwrap())
            }};
        }
        let result = match instr {
            CoroInstruction::Simple(node) => match node.get().instruction.as_ref() {
                Instruction::Call(func, _) => match func {
                    Func::Unreachable(_) => true,
                    _ => false,
                },
                _ => false,
            },
            CoroInstruction::EntryScope { body } => {
                let result = traverse!(body);
                assert!(result);
                true
            }
            CoroInstruction::Entry
            | CoroInstruction::Suspend { .. }
            | CoroInstruction::Terminate => true,
            CoroInstruction::Loop { body, .. } => {
                traverse!(body)
            }
            CoroInstruction::If {
                true_branch,
                false_branch,
                ..
            } => {
                let true_is_terminator = traverse!(true_branch);
                let false_is_terminator = traverse!(false_branch);
                true_is_terminator && false_is_terminator
            }
            CoroInstruction::Switch { cases, default, .. } => {
                let mut cases_are_all_terminators = true;
                for case in cases.iter() {
                    let case_is_terminator = traverse!(&case.body);
                    cases_are_all_terminators = cases_are_all_terminators && case_is_terminator;
                }
                let default_is_terminator = traverse!(default);
                cases_are_all_terminators && default_is_terminator
            }
            _ => false,
        };
        known.insert(*instr_ref, result);
    }

    fn from(module: &Module) -> Self {
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
        // find terminators
        let mut terminators = HashMap::new();
        Self::find_terminators(&instructions, &entry, &mut terminators);
        assert!((0..instructions.len()).all(|instr| {
            let instr = CoroInstrRef(instr);
            terminators.contains_key(&instr)
        }));
        let terminators: HashSet<_> = terminators
            .iter()
            .filter_map(|(k, v)| if *v { Some(*k) } else { None })
            .collect();
        // construct the graph
        Self {
            entry_scope: entry,
            instructions,
            node_to_instr,
            terminators,
        }
    }
}

// This struct is the final coroutine graph after splitting the coroutine scopes.
#[derive(Debug)]
pub(crate) struct CoroGraph {
    pub scopes: Vec<CoroScope>,              // all the scopes in the graph
    pub entry: CoroScopeRef,                 // the index of the entry scope (the root scope)
    pub tokens: BTreeMap<u32, CoroScopeRef>, // map from split mark token to scope index
    pub instructions: Vec<CoroInstruction>,  // all the instructions in the graph
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
struct CoroGraphIndexer {
    parent: CoroInstrRef,
    parent_branch: usize,
    index_in_parent_branch: usize,
}

impl CoroGraphIndexer {
    fn get_parent_branch<'a>(
        &self,
        instructions: &'a Vec<CoroInstruction>,
    ) -> &'a Vec<CoroInstrRef> {
        let parent_instr = instructions.get(self.parent.0).unwrap();
        match parent_instr {
            CoroInstruction::EntryScope { body } => {
                assert_eq!(self.parent_branch, 0);
                body
            }
            CoroInstruction::Loop { body, .. } => {
                assert_eq!(self.parent_branch, 0);
                body
            }
            CoroInstruction::If {
                true_branch,
                false_branch,
                ..
            } => match self.parent_branch {
                0 => true_branch,
                1 => false_branch,
                _ => panic!("Unexpected branch index."),
            },
            CoroInstruction::Switch { cases, default, .. } => {
                if self.parent_branch < cases.len() {
                    &cases[self.parent_branch].body
                } else {
                    assert_eq!(self.parent_branch, cases.len());
                    default
                }
            }
            _ => panic!("Unexpected instruction."),
        }
    }

    fn get_instr_ref(&self, instructions: &Vec<CoroInstruction>) -> CoroInstrRef {
        self.get_parent_branch(instructions)
            .get(self.index_in_parent_branch)
            .unwrap()
            .clone()
    }

    fn get_instr<'a>(&self, instructions: &'a Vec<CoroInstruction>) -> &'a CoroInstruction {
        let index = self.get_instr_ref(instructions).0;
        instructions.get(index).unwrap()
    }
}

impl CoroPreliminaryGraph {
    fn instr(&self, i: CoroInstrRef) -> &CoroInstruction {
        &self.instructions[i.0]
    }

    fn is_terminator(&self, i: CoroInstrRef) -> bool {
        self.terminators.contains(&i)
    }

    fn get(&self, i: CoroGraphIndexer) -> CoroInstrRef {
        i.get_instr_ref(&self.instructions).clone()
    }

    fn get_instr(&self, i: CoroGraphIndexer) -> &CoroInstruction {
        i.get_instr(&self.instructions)
    }

    fn get_parent_branch(&self, i: CoroGraphIndexer) -> &Vec<CoroInstrRef> {
        i.get_parent_branch(&self.instructions)
    }
}

impl CoroGraph {
    fn add_instr(&mut self, instr: CoroInstruction) -> CoroInstrRef {
        let i = self.instructions.len();
        self.instructions.push(instr);
        CoroInstrRef(i)
    }

    fn add_scope(&mut self, scope: CoroScope) -> CoroScopeRef {
        let i = self.scopes.len();
        self.scopes.push(scope);
        CoroScopeRef(i)
    }

    fn instr_mut(&mut self, i: CoroInstrRef) -> &mut CoroInstruction {
        &mut self.instructions[i.0]
    }

    fn replay_condition_stack(
        graph: &mut CoroGraph,
        preliminary: &CoroPreliminaryGraph,
        ancestors: &Vec<CoroGraphIndexer>,
        subscope: &mut CoroScope,
    ) {
        let stack: Vec<_> = ancestors
            .iter()
            .filter_map(|ancestor| {
                let instr = preliminary.instr(ancestor.parent);
                match instr {
                    CoroInstruction::If { cond, .. } => {
                        if let CoroInstruction::Simple(node) = preliminary.instr(*cond) {
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
                        if let CoroInstruction::Simple(node) = preliminary.instr(*cond) {
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
            let replay = graph.add_instr(CoroInstruction::ConditionStackReplay { items: stack });
            subscope.instructions.push(replay);
        }
    }

    fn clone_instruction(
        graph: &mut CoroGraph,
        preliminary: &CoroPreliminaryGraph,
        instr_ref: CoroInstrRef,
    ) -> CoroInstrRef {
        macro_rules! clone_block {
            ($block: expr) => {{
                let mut block = Vec::new();
                for &instr_ref in $block.iter() {
                    block.push(Self::clone_instruction(graph, preliminary, instr_ref));
                    if preliminary.is_terminator(instr_ref) {
                        break;
                    }
                }
                block
            }};
        }
        match preliminary.instr(instr_ref) {
            CoroInstruction::Simple(_)
            | CoroInstruction::Suspend { .. }
            | CoroInstruction::Terminate => instr_ref,
            CoroInstruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                let true_branch = clone_block!(true_branch);
                let false_branch = clone_block!(false_branch);
                graph.add_instr(CoroInstruction::If {
                    cond: *cond,
                    true_branch,
                    false_branch,
                })
            }
            CoroInstruction::Loop { body, cond } => {
                let body = clone_block!(body);
                graph.add_instr(CoroInstruction::Loop { body, cond: *cond })
            }
            CoroInstruction::Switch {
                cond,
                cases,
                default,
            } => {
                let cases = cases
                    .iter()
                    .map(|case| CoroSwitchCase {
                        value: case.value,
                        body: clone_block!(&case.body),
                    })
                    .collect();
                let default = clone_block!(default);
                graph.add_instr(CoroInstruction::Switch {
                    cond: *cond,
                    cases,
                    default,
                })
            }
            _ => unreachable!("Invalid instruction."),
        }
    }

    fn construct_subscope_for_ancestors<'a>(
        graph: &mut CoroGraph,
        preliminary: &CoroPreliminaryGraph,
        suspend: CoroGraphIndexer,
        stack: &'a [CoroGraphIndexer],
        first_flag: CoroInstrRef,
        inside_loop: bool,
        block: &mut Vec<CoroInstrRef>,
    ) {
        // end of the recursion
        if stack.is_empty() {
            if inside_loop {
                let suspend = preliminary.get(suspend);
                if let Some(CoroInstruction::SkipIfFirstFlag { flag, body }) =
                    block.last().map(|i| graph.instr_mut(*i))
                {
                    assert_eq!(flag, &first_flag);
                    body.push(suspend);
                } else {
                    block.push(graph.add_instr(CoroInstruction::SkipIfFirstFlag {
                        flag: first_flag,
                        body: vec![suspend],
                    }));
                }
            }
            block.push(graph.add_instr(CoroInstruction::ClearFirstFlag(first_flag)));
            return;
        }
        macro_rules! clone_branch {
            ($body: expr, $dest: expr, $begin: expr, $end: expr) => {
                for &instr_ref in $body.iter().take($end).skip($begin) {
                    $dest.push(Self::clone_instruction(graph, preliminary, instr_ref));
                    if preliminary.is_terminator(instr_ref) {
                        break;
                    }
                }
            };
        }

        // simple case: not inside a loop, just append the succeeding instructions
        let current = stack[0];

        macro_rules! clone_suspend_branch {
            ($branch: expr) => {{
                let mut preceding = Vec::new();
                clone_branch!($branch, preceding, 0, current.index_in_parent_branch);
                let masked = graph.add_instr(CoroInstruction::SkipIfFirstFlag {
                    flag: first_flag,
                    body: preceding,
                });
                let mut cloned_suspend_block = vec![masked];
                let inside_loop = inside_loop
                    || match preliminary.instr(current.parent) {
                        CoroInstruction::Loop { .. } => true,
                        _ => false,
                    };
                Self::construct_subscope_for_ancestors(
                    graph,
                    preliminary,
                    suspend,
                    &stack[1..],
                    first_flag,
                    inside_loop,
                    &mut cloned_suspend_block,
                );
                clone_branch!(
                    $branch,
                    cloned_suspend_block,
                    current.index_in_parent_branch + 1,
                    $branch.len()
                );
                cloned_suspend_block
            }};
        }

        macro_rules! clone_non_suspend_branch {
            ($branch: expr) => {{
                let mut cloned_non_suspend_block = Vec::new();
                clone_branch!($branch, cloned_non_suspend_block, 0, $branch.len());
                cloned_non_suspend_block
            }};
        }

        match preliminary.instr(current.parent) {
            CoroInstruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                if inside_loop {
                    let (true_branch, false_branch) = match current.parent_branch {
                        0 => (
                            clone_suspend_branch!(true_branch),
                            clone_non_suspend_branch!(false_branch),
                        ),
                        1 => (
                            clone_non_suspend_branch!(true_branch),
                            clone_suspend_branch!(false_branch),
                        ),
                        _ => panic!("Unexpected branch index."),
                    };
                    block.push(graph.add_instr(CoroInstruction::If {
                        cond: *cond,
                        true_branch,
                        false_branch,
                    }));
                } else {
                    let parent_branch = preliminary.get_parent_branch(current);
                    clone_branch!(
                        parent_branch,
                        block,
                        current.index_in_parent_branch + 1,
                        parent_branch.len()
                    );
                }
            }
            CoroInstruction::Switch {
                cond,
                cases,
                default,
            } => {
                if inside_loop {
                    assert!(current.index_in_parent_branch <= cases.len());
                    let cases: Vec<_> = cases
                        .iter()
                        .enumerate()
                        .map(|(i, case)| {
                            let body = if i == current.parent_branch {
                                clone_suspend_branch!(&case.body)
                            } else {
                                clone_non_suspend_branch!(&case.body)
                            };
                            CoroSwitchCase {
                                value: case.value,
                                body,
                            }
                        })
                        .collect();
                    let default = if cases.len() == current.parent_branch {
                        clone_suspend_branch!(default)
                    } else {
                        clone_non_suspend_branch!(default)
                    };
                    block.push(graph.add_instr(CoroInstruction::Switch {
                        cond: *cond,
                        cases,
                        default,
                    }));
                } else {
                    let parent_branch = preliminary.get_parent_branch(current);
                    clone_branch!(
                        parent_branch,
                        block,
                        current.index_in_parent_branch + 1,
                        parent_branch.len()
                    );
                }
            }
            CoroInstruction::Loop { body, cond } => {
                let body = clone_suspend_branch!(body);
                block.push(graph.add_instr(CoroInstruction::Loop { body, cond: *cond }));
            }
            _ => panic!("Unexpected instruction."),
        }
    }

    fn remove_unreachable_from_instructions(graph: &mut CoroGraph, instr: CoroInstrRef) -> bool /* whether the block is terminated */
    {
        unsafe {
            // Sorry, Rust, but this is really safe.
            let p_graph = &mut *(graph as *mut CoroGraph);
            match p_graph.instr_mut(instr) {
                CoroInstruction::Simple(node) => match node.get().instruction.as_ref() {
                    Instruction::Call(func, _) => match func {
                        Func::Unreachable(_) => true,
                        _ => false,
                    },
                    _ => false,
                },
                CoroInstruction::Loop { body, .. } => {
                    Self::remove_unreachable_from_block(graph, body)
                }
                CoroInstruction::SkipIfFirstFlag { body, .. } => {
                    Self::remove_unreachable_from_block(graph, body);
                    false
                }
                CoroInstruction::If {
                    true_branch,
                    false_branch,
                    ..
                } => {
                    let true_terminated = Self::remove_unreachable_from_block(graph, true_branch);
                    let false_terminated = Self::remove_unreachable_from_block(graph, false_branch);
                    true_terminated && false_terminated
                }
                CoroInstruction::Switch { cases, default, .. } => {
                    let mut cases_terminated = true;
                    for case in cases.iter_mut() {
                        let terminated = Self::remove_unreachable_from_block(graph, &mut case.body);
                        cases_terminated = cases_terminated && terminated;
                    }
                    let default_terminated = Self::remove_unreachable_from_block(graph, default);
                    cases_terminated && default_terminated
                }
                CoroInstruction::Suspend { .. } | CoroInstruction::Terminate => true,
                _ => false,
            }
        }
    }

    fn remove_unreachable_from_block(graph: &mut CoroGraph, block: &mut Vec<CoroInstrRef>) -> bool {
        if let Some(terminated_index) = block
            .iter()
            .position(|&instr| Self::remove_unreachable_from_instructions(graph, instr))
        {
            block.truncate(terminated_index + 1);
            // if the last terminator instruction is a `loop`, then we can just take its body
            if let Some(CoroInstruction::Loop { body, cond }) =
                block.last().map(|i| &graph.instructions[i.0])
            {
                block.pop();
                block.extend(body);
            }
            true
        } else {
            false
        }
    }

    fn construct_subscope(
        graph: &mut CoroGraph,
        preliminary: &CoroPreliminaryGraph,
        current: CoroGraphIndexer,
        ancestors: &Vec<CoroGraphIndexer>,
    ) -> CoroScope {
        let mut subscope = CoroScope {
            instructions: Vec::new(),
        };
        let mut stack = ancestors.clone();
        stack.push(current);
        // replay the condition stack
        Self::replay_condition_stack(graph, preliminary, &stack, &mut subscope);
        // process the non-directly dominated instructions
        let first_flag = graph.add_instr(CoroInstruction::MakeFirstFlag);
        subscope.instructions.push(first_flag);
        Self::construct_subscope_for_ancestors(
            graph,
            preliminary,
            current,
            &stack[1..],
            first_flag,
            false,
            &mut subscope.instructions,
        );
        // add the instructions in the outermost scope
        let outermost = stack[0];
        let outermost_parent_branch = preliminary.get_parent_branch(outermost);
        for &i in outermost_parent_branch
            .iter()
            .skip(outermost.index_in_parent_branch + 1)
        {
            subscope
                .instructions
                .push(Self::clone_instruction(graph, preliminary, i));
            if preliminary.is_terminator(i) {
                break;
            }
        }
        // minor simplifications
        Self::remove_unreachable_from_block(graph, &mut subscope.instructions);
        subscope
    }

    fn terminated_in_current_branch(
        preliminary: &CoroPreliminaryGraph,
        current: CoroGraphIndexer,
    ) -> bool {
        preliminary
            .get_parent_branch(current)
            .iter()
            .skip(current.index_in_parent_branch + 1)
            .any(|&instr_ref| preliminary.is_terminator(instr_ref))
    }

    fn find_reachable_ancestors(
        preliminary: &CoroPreliminaryGraph,
        current: CoroGraphIndexer,
        ancestors: &Vec<CoroGraphIndexer>,
    ) -> Vec<CoroGraphIndexer> {
        let mut reachable_ancestors = Vec::new();
        let mut instr = current;
        for ancestor in ancestors.iter().rev() {
            // if terminated in the current branch, then only the remaining ancestors are unreachable
            if Self::terminated_in_current_branch(preliminary, instr) {
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
        preliminary: &CoroPreliminaryGraph,
        current: CoroGraphIndexer,
        ancestors: &Vec<CoroGraphIndexer>,
    ) {
        let instr = preliminary.get_instr(current);
        let token = match instr {
            CoroInstruction::Entry => None,
            CoroInstruction::Suspend { token } => Some(*token),
            _ => panic!("Unexpected instruction."),
        };
        if let Some(token) = token {
            if graph.tokens.contains_key(&token) {
                // the continuation has been extracted
                return;
            }
        }
        // actually extract the continuation
        let ancestors = Self::find_reachable_ancestors(preliminary, current, ancestors);
        let subscope = Self::construct_subscope(graph, preliminary, current, &ancestors);
        // add the sub-scope to the graph
        let subscope_ref = graph.add_scope(subscope);
        if let Some(token) = token {
            graph.tokens.insert(token, subscope_ref);
        } else {
            graph.entry = subscope_ref;
        }
    }

    fn recurse_continuation_extraction(
        graph: &mut CoroGraph,
        preliminary: &CoroPreliminaryGraph,
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
                        preliminary,
                        instr_ref,
                        $branch,
                        $block,
                        ancestors,
                    );
                    let popped = ancestors.pop();
                    assert_eq!(popped, Some(current));
                };
            }
            let instr = preliminary.instr(instr_ref);
            match instr {
                CoroInstruction::Entry | CoroInstruction::Suspend { .. } => {
                    // a split mark is encountered
                    Self::extract_continuation_at_suspend(
                        graph,
                        preliminary,
                        CoroGraphIndexer {
                            parent: parent_scope,
                            parent_branch,
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
        // create a graph
        let mut graph = CoroGraph {
            scopes: Vec::new(),
            entry: CoroScopeRef::invalid(),
            tokens: BTreeMap::new(),
            // clone so that the indices are not changed when we add new instructions
            instructions: preliminary_graph.instructions.clone(),
        };
        let entry = preliminary_graph.instr(preliminary_graph.entry_scope);
        if let CoroInstruction::EntryScope { body: entry_body } = entry {
            Self::recurse_continuation_extraction(
                &mut graph,
                &preliminary_graph,
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

impl CoroGraph {
    fn dump_instr(&self, indent: usize, instr_ref: CoroInstrRef) {
        macro_rules! print_indent {
            ($n: expr) => {
                for _ in 0..$n {
                    print!("    ");
                }
            };
        }
        macro_rules! print_body {
            ($body: expr, $indent: expr) => {
                println!("{{");
                let indent = $indent + 1;
                for instr_ref in $body.iter() {
                    self.dump_instr(indent, *instr_ref);
                }
                print_indent!(indent - 1);
                print!("}}");
            };
        }
        match &self.instructions[instr_ref.0] {
            CoroInstruction::Simple(node) => match node.get().instruction.as_ref() {
                Instruction::Comment(msg) => {
                    for line in msg.to_string().lines() {
                        print_indent!(indent);
                        println!("// {}", line);
                    }
                }
                _ => {
                    print_indent!(indent);
                    println!("${} = Simple({:?})", instr_ref.0, node);
                }
            },
            CoroInstruction::ConditionStackReplay { items } => {
                print_indent!(indent);
                println!("ConditionStackReplay {{");
                let indent = indent + 1;
                for item in items.iter() {
                    print_indent!(indent);
                    println!("{:?} = {}", item.node, item.value);
                }
                print_indent!(indent - 1);
                println!("}}");
            }
            CoroInstruction::MakeFirstFlag => {
                print_indent!(indent);
                println!("${} = MakeFirstFlag", instr_ref.0);
            }
            CoroInstruction::SkipIfFirstFlag { flag, body } => {
                print_indent!(indent);
                print!("SkipIfFirstFlag(${}) ", flag.0);
                print_body!(body, indent);
                println!();
            }
            CoroInstruction::ClearFirstFlag(flag) => {
                print_indent!(indent);
                println!("ClearFirstFlag(${})", flag.0);
            }
            CoroInstruction::Loop { body, cond } => {
                print_indent!(indent);
                print!("Loop ");
                print_body!(body, indent);
                println!(" While (${})", cond.0);
            }
            CoroInstruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                print_indent!(indent);
                print!("If (${}) ", cond.0);
                print_body!(true_branch, indent);
                if !false_branch.is_empty() {
                    print!(" Else ");
                    print_body!(false_branch, indent);
                }
                println!();
            }
            CoroInstruction::Switch {
                cond,
                cases,
                default,
            } => {
                print_indent!(indent);
                print!("Switch (${}) {{", cond.0);
                let indent = indent + 1;
                for case in cases.iter() {
                    print_indent!(indent);
                    print!("Case {} ", case.value);
                    print_body!(&case.body, indent);
                    println!();
                }
                print_indent!(indent);
                print!("Default ");
                print_body!(default, indent);
                println!();
                print_indent!(indent - 1);
                println!("}}");
            }
            CoroInstruction::Suspend { token } => {
                print_indent!(indent);
                println!("Suspend({})", token);
            }
            CoroInstruction::Terminate => {
                print_indent!(indent);
                println!("Terminate");
            }
            _ => panic!("Unexpected instruction."),
        }
    }

    fn dump_scope(&self, scope: &CoroScope) {
        for instr_ref in scope.instructions.iter() {
            self.dump_instr(0, *instr_ref);
        }
    }
}

// public API
impl CoroGraph {
    pub fn from(module: &Module) -> Self {
        let preliminary_graph = CoroPreliminaryGraph::from(module);
        Self::build(preliminary_graph)
    }

    pub fn get_scope(&self, scope: CoroScopeRef) -> &CoroScope {
        &self.scopes[scope.0]
    }

    pub fn get_instr(&self, instr: CoroInstrRef) -> &CoroInstruction {
        &self.instructions[instr.0]
    }

    pub fn dump(&self) {
        println!("====================== CoroGraph ======================");
        println!("Subscope count: {}", self.scopes.len());
        println!("Entry scope: {}", self.entry.0);
        println!("Tokens: {:?}", self.tokens);
        println!();
        println!("===================== Entry Scope =====================");
        self.dump_scope(&self.scopes[self.entry.0]);
        for (token, scope) in self.tokens.iter() {
            println!();
            println!(
                "===================== Subscope {} =====================",
                token
            );
            self.dump_scope(&self.scopes[scope.0]);
        }
    }
}
