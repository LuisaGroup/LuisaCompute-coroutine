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
