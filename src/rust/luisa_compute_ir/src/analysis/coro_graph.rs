// This file implements the coroutine graph extraction analysis, which traverse the IR to
// find all coroutine split marks and store them into a graph. Each graph node records the
// corresponding instructions that the subroutine contains. A subroutine is a sequence of
// instructions between two coroutine split marks, i.e., instructions from one split mark
// through all the reachable instructions until the next split mark is encountered.
// Note: this analysis only works on a single module without recurse into its callees.

use crate::ir::{BasicBlock, Instruction, Module, NodeRef};
use std::collections::HashMap;

pub(crate) struct CoroSwitchCase {
    pub value: i64,
    pub body: Vec<usize>, // indices into the graph nodes
}

pub(crate) enum CoroInstruction {
    // A simple IR node that does not contain any nested basic blocks.
    Simple(NodeRef),

    // A do-while loop
    Loop {
        body: Vec<usize>, // indices into the graph nodes
        cond: usize,      // indices into the graph nodes
    },

    // An if-then-else branch
    If {
        cond: usize,              // indices into the graph nodes
        true_branch: Vec<usize>,  // indices into the graph nodes
        false_branch: Vec<usize>, // indices into the graph nodes
    },

    // A switch statement
    Switch {
        value: usize,               // indices into the graph nodes
        cases: Vec<CoroSwitchCase>, // indices into the graph nodes
        default: Vec<usize>,        // indices into the graph nodes
    },

    // An suspend mark
    Suspend {
        token: u32,
    },

    // A terminate mark
    Terminate,
}

pub(crate) struct CoroScope {
    pub instructions: Vec<usize>, // indices into the graph nodes
}

// This struct is a direct translation from the IR to the coroutine graph without
// splitting the coroutine scopes, i.e., the only scope is the root (entry) scope.
pub(crate) struct CoroPreliminaryGraph {
    pub scope: CoroScope,                       // the current scope
    pub instructions: Vec<CoroInstruction>,     // all the instructions in the graph
    pub node_to_instr: HashMap<NodeRef, usize>, // IR node to instruction index
}

impl CoroPreliminaryGraph {
    fn translate_node(
        node: &NodeRef,
        instructions: &mut Vec<CoroInstruction>,
        node_to_instr: &mut HashMap<NodeRef, usize>,
    ) -> usize {
        assert!(!node_to_instr.contains_key(node), "Duplicate node in IR.");
        macro_rules! register {
            ($node: expr, $instr: expr) => {{
                let index = instructions.len();
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
                        value: case.value as i64,
                        body: Self::translate_block(&case.block, instructions, node_to_instr),
                    })
                    .collect();
                let default = Self::translate_block(default, instructions, node_to_instr);
                let instr = CoroInstruction::Switch {
                    value,
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
        node_to_instr: &mut HashMap<NodeRef, usize>,
    ) -> Vec<usize> {
        block
            .iter()
            .map(|node| Self::translate_node(&node, instructions, node_to_instr))
            .collect()
    }

    pub fn from(module: &Module) -> Self {
        let mut instructions = Vec::new();
        let mut node_to_instr = HashMap::new();
        let entry =
            Self::translate_block(module.entry.as_ref(), &mut instructions, &mut node_to_instr);
        Self {
            scope: CoroScope {
                instructions: entry,
            },
            instructions,
            node_to_instr,
        }
    }
}

// This struct is the final coroutine graph after splitting the coroutine scopes.
pub(crate) struct CoroGraph {
    scopes: Vec<CoroScope>,                 // all the scopes in the graph
    entry: usize, // the index of the scope that contains the entry split mark
    marks: HashMap<u32, usize>, // map from split mark token to scope index
    instructions: Vec<CoroInstruction>, // all the instructions in the graph
    node_to_instr: HashMap<NodeRef, usize>, // IR node to instruction index
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
//       first_flag = false; // the `first` flag is assigned to `false` at the end of the loop body
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
