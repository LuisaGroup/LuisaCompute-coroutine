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
//    a. the variable has not been demoted by the node's ancestor, and
//       b.1 the variable is found propagated to more than one child node, or
//       b.2 the variable is found directly referenced in the node.
// 4. Move variable definitions to proper places and remove unreferenced variables if any.
//
// Note: the `reg2mem` transform should be applied before this transform.

use crate::analysis::scope_tree::{ScopeTree, ScopeTreeBlock, ScopeTreeNode};
use crate::ir::{BasicBlock, Instruction, Module, Node, NodeRef};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

pub struct DemoteLocals;

struct VariablePropagator;

struct VariableCollection {
    direct_refs: HashSet<NodeRef>,
    propagated_refs: HashSet<NodeRef>,
}

struct VariablePropagation {
    collections: HashMap<*const BasicBlock, VariableCollection>,
}

// Implementation of Step 2: propagate referenced variables to parent nodes.
// The result is a map from basic blocks to the variables defined in the block.
// To get the variables defined in a node, simply traverse the internal blocks of the node.
// The `ScopeTreeNode::blocks` field already has recorded the nested blocks in the node.
// Rule:
//   propagated_refs = child.direct_refs + child.propagated_refs - ancestor_direct_refs
// Note:
//   we don't need to propagate variables defined in the node itself and its ancestors as
//   they are anyways propagated to the parent.
impl VariablePropagator {
    fn propagate_block(
        tree: &ScopeTree,
        tree_block: &ScopeTreeBlock,
        ancestor_direct_refs: &HashSet<NodeRef>, // should be excluded from propagation
        propagation: &mut VariablePropagation,
    ) {
        let mut collection = VariableCollection {
            direct_refs: HashSet::new(),
            propagated_refs: HashSet::new(),
        };
        // collect direct references
        for node in tree_block.block.iter() {
            Self::collect_node_direct_refs(
                &node,
                &mut collection.direct_refs,
                &ancestor_direct_refs,
                false,
            );
        }
        // propagate references from children
        if !tree_block.children.is_empty() {
            // update ancestor_direct_refs to include the current block
            let mut ancestor_direct_refs = collection.direct_refs.clone();
            ancestor_direct_refs.extend(&collection.direct_refs);
            // traverse children
            for &i in tree_block.children.iter() {
                for child in tree.nodes[i].blocks.iter() {
                    Self::propagate_block(tree, child, &ancestor_direct_refs, propagation);
                    // merge propagated_refs
                    let child_collection =
                        propagation.collections.get(&child.block.as_ptr()).unwrap();
                    collection
                        .propagated_refs
                        .extend(&child_collection.direct_refs);
                    collection
                        .propagated_refs
                        .extend(&child_collection.propagated_refs);
                }
            }
        }
        propagation
            .collections
            .insert(tree_block.block.as_ptr(), collection);
    }

    fn collect_node_direct_refs(
        node: &NodeRef,
        direct_refs: &mut HashSet<NodeRef>,
        ancestor_direct_refs: &HashSet<NodeRef>, // should be excluded from propagation as they are anyways propagated to the parent
        is_referenced: bool,
    ) {
        macro_rules! check_not_local {
            ($node: expr) => {
                assert!(!$node.is_local(), "local variable not loaded");
            };
        }
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
                    if !ancestor_direct_refs.contains(node) {
                        direct_refs.insert(node.clone());
                    }
                } else {
                    check_not_local!(init);
                }
            }
            Instruction::Argument { .. } => {}
            Instruction::UserData(_) => {}
            Instruction::Invalid => {}
            Instruction::Const(_) => {}
            Instruction::Update { var, value } => {
                Self::collect_node_direct_refs(var, direct_refs, ancestor_direct_refs, true);
                check_not_local!(value);
            }
            Instruction::Call(_, args) => {
                for arg in args.iter() {
                    Self::collect_node_direct_refs(arg, direct_refs, ancestor_direct_refs, true);
                }
            }
            Instruction::Phi(_) => panic!("phi node not supported"),
            Instruction::Return(value) => {
                check_not_local!(value);
            }
            Instruction::Loop { cond, .. } => {
                check_not_local!(cond);
            }
            Instruction::GenericLoop { cond, .. } => {
                check_not_local!(cond);
            }
            Instruction::Break => {}
            Instruction::Continue => {}
            Instruction::If { cond, .. } => {
                check_not_local!(cond);
            }
            Instruction::Switch { value, .. } => {
                check_not_local!(value);
            }
            Instruction::AdScope { .. } => {}
            Instruction::RayQuery { ray_query, .. } => {
                Self::collect_node_direct_refs(ray_query, direct_refs, ancestor_direct_refs, true);
            }
            Instruction::Print { args, .. } => {
                for arg in args.iter() {
                    Self::collect_node_direct_refs(arg, direct_refs, ancestor_direct_refs, true);
                }
            }
            Instruction::AdDetach(_) => {}
            Instruction::Comment(_) => {}
            Instruction::CoroSplitMark { .. } => {}
            Instruction::CoroSuspend { .. } => {}
            Instruction::CoroResume { .. } => {}
            Instruction::CoroRegister { value, .. } => {
                check_not_local!(value);
            }
        }
    }

    pub fn propagate(tree: &ScopeTree) -> VariablePropagation {
        assert_eq!(tree.root().blocks.len(), 1);
        let mut prop = VariablePropagation {
            collections: HashMap::new(),
        };
        let ancestor_direct_refs = HashSet::new();
        Self::propagate_block(
            &tree,
            &tree.root().blocks[0],
            &ancestor_direct_refs,
            &mut prop,
        );
        prop
    }
}
