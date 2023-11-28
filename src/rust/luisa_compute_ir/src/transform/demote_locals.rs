// This file implements the local variable demotion transform.
//
// This transform demotes local variables to its lowest possible scope, i.e., to right
// before the lowest common ancestor of all its references, in the following steps:
// 1. Construct a scope tree from the AST of the module, where each node is a instruction
//    that contains nested basic blocks (using the `scope_tree` analysis).
// 2. Recursively visit each node in the scope tree and propagate the referenced variables
//    to its parent node.
// 3. At each node for each propagated variable, demote the variable definition here if
//    following conditions are met:
//    a) the variable has not been demoted by the node's ancestor, and
//    b) the variable is found propagated to more than one child node.
// 4. Move variable definitions to proper places and remove unreferenced variables if any.
//
// Note: the `reg2mem` transform should be applied before this transform.

use crate::analysis::scope_tree::{ScopeTree, ScopeTreeBlock, ScopeTreeNode};
use crate::ir::{BasicBlock, Instruction, Module, NodeRef};
use std::collections::{HashMap, HashSet};

pub struct DemoteLocals;

struct VariablePropagator;

// Implementation of Step 2: propagate referenced variables to parent nodes.
// The result is a map from basic blocks to the variables defined in the block.
// To get the variables defined in a node, simply traverse the internal blocks of the node.
// The `ScopeTreeNode::blocks` field already has recorded the nested blocks in the node.
impl VariablePropagator {
    fn propagate_tree_block(
        tree: &ScopeTree,
        tree_block: &ScopeTreeBlock,
        vars: &mut HashMap<*const BasicBlock, HashSet<NodeRef>>,
    ) {
        // children nodes with blocks requires special handling
        let block_nodes: HashMap<_, _> = tree_block
            .children
            .iter()
            .map(|&i| (tree.nodes[i].node.clone(), i))
            .collect();
        let bb = &tree_block.block;
        let mut block_vars = HashSet::new();
        for node in bb.iter() {
            Self::propagate_node(&node, &mut block_vars, false);
            if let Some(block_node) = block_nodes.get(&node) {
                Self::propagate_tree_node(tree, *block_node, &mut block_vars, vars);
            }
        }
        vars.insert(bb.as_ptr(), block_vars);
    }

    fn propagate_node(node: &NodeRef, vars: &mut HashSet<NodeRef>, is_referenced: bool) {
        match node.get().instruction.as_ref() {
            Instruction::Buffer => {}
            Instruction::Bindless => {}
            Instruction::Texture2D => {}
            Instruction::Texture3D => {}
            Instruction::Accel => {}
            Instruction::Shared => panic!("shared memory not supported"),
            Instruction::Uniform => {}
            Instruction::Local { init } => {
                if is_referenced {
                    vars.insert(node.clone());
                } else {
                    assert!(init.valid() && !init.is_local());
                }
            }
            Instruction::Argument { .. } => {}
            Instruction::UserData(_) => {}
            Instruction::Invalid => {}
            Instruction::Const(_) => {}
            Instruction::Update { var, value } => {
                Self::propagate_node(var, vars, true);
                assert!(value.valid() && !value.is_local());
            }
            Instruction::Call(_, args) => {
                for arg in args.iter() {
                    Self::propagate_node(arg, vars, true);
                }
            }
            Instruction::Phi(_) => panic!("phi node not supported"),
            Instruction::Return(value) => {
                assert!(!value.valid() || !value.is_local());
            }
            Instruction::Loop { cond, .. } => {
                assert!(cond.valid() && !cond.is_local());
            }
            Instruction::GenericLoop { cond, .. } => {
                assert!(cond.valid() && !cond.is_local());
            }
            Instruction::Break => {}
            Instruction::Continue => {}
            Instruction::If { cond, .. } => {
                assert!(cond.valid() && !cond.is_local());
            }
            Instruction::Switch { value, .. } => {
                assert!(value.valid() && !value.is_local());
            }
            Instruction::AdScope { .. } => {}
            Instruction::RayQuery { ray_query, .. } => {
                Self::propagate_node(ray_query, vars, true);
            }
            Instruction::Print { args, .. } => {
                for arg in args.iter() {
                    Self::propagate_node(arg, vars, true);
                }
            }
            Instruction::AdDetach(_) => {}
            Instruction::Comment(_) => {}
            Instruction::CoroSplitMark { .. } => {}
            Instruction::CoroSuspend { .. } => {}
            Instruction::CoroResume { .. } => {}
            Instruction::CoroRegister { value, .. } => {
                assert!(value.valid() && !value.is_local());
            }
        }
    }

    fn propagate_tree_node(
        tree: &ScopeTree,
        tree_node: usize,
        node_vars: &mut HashSet<NodeRef>,
        vars: &mut HashMap<*const BasicBlock, HashSet<NodeRef>>,
    ) {
        for block in &tree.nodes[tree_node].blocks {
            Self::propagate_tree_block(tree, block, vars);
            node_vars.extend(vars.get(&block.block.as_ptr()).unwrap());
        }
    }

    pub fn propagate(tree: &ScopeTree) -> HashMap<*const BasicBlock, HashSet<NodeRef>> {
        let mut block_vars = HashMap::new();
        assert_eq!(tree.root().blocks.len(), 1);
        Self::propagate_tree_block(&tree, &tree.root().blocks[0], &mut block_vars);
        block_vars
    }
}
