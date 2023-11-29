// This file implements the local variable demotion transform.
//
// This transform demotes local variables to its lowest possible scope, i.e., to right
// before the lowest common ancestor of all its references, in the following steps:
// 1. Construct a scope tree from the AST of the module, where each node is a instruction
//    that contains nested basic blocks (using the `scope_tree` analysis).
// 2. Recursively visit each node in the scope tree and propagate the referenced variables
//    to its parent node.
// 3. At each node for each propagated variable, demote the variable definition here if
//    the following conditions are met:
//    a. the variable has not been demoted by the node's ancestor, and
//       b.1 the variable is found propagated to more than one child node, or
//       b.2 the variable is found directly referenced in the node.
// 4. Move variable definitions to proper places and remove unreferenced variables if any.
//
// Note: the `reg2mem` transform should be applied before this transform.

use crate::analysis::scope_tree::{ScopeTree, ScopeTreeBlock};
use crate::ir::debug::dump_ir_human_readable;
use crate::ir::{BasicBlock, Instruction, Module, NodeRef};
use crate::transform::Transform;
use std::collections::{HashMap, HashSet};

pub struct DemoteLocals;

struct VariablePropagator;

struct VariableCollection {
    direct_refs: HashSet<NodeRef>,
    propagated_refs: HashSet<NodeRef>,
}

impl VariableCollection {
    fn contains(&self, node: &NodeRef) -> bool {
        self.direct_refs.contains(node) || self.propagated_refs.contains(node)
    }
}

struct VariablePropagation {
    indices: HashMap<NodeRef, usize>,
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
                propagation,
                false,
            );
        }
        // propagate references from children
        if !tree_block.children.is_empty() {
            // update ancestor_direct_refs to include the current block
            let mut ancestor_direct_refs = ancestor_direct_refs.clone();
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
        propagation: &mut VariablePropagation,
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
                        let index = propagation.indices.len();
                        propagation.indices.get(&node).get_or_insert(&index);
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
                Self::collect_node_direct_refs(
                    var,
                    direct_refs,
                    ancestor_direct_refs,
                    propagation,
                    true,
                );
                check_not_local!(value);
            }
            Instruction::Call(_, args) => {
                for arg in args.iter() {
                    Self::collect_node_direct_refs(
                        arg,
                        direct_refs,
                        ancestor_direct_refs,
                        propagation,
                        true,
                    );
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
                Self::collect_node_direct_refs(
                    ray_query,
                    direct_refs,
                    ancestor_direct_refs,
                    propagation,
                    true,
                );
            }
            Instruction::Print { args, .. } => {
                for arg in args.iter() {
                    Self::collect_node_direct_refs(
                        arg,
                        direct_refs,
                        ancestor_direct_refs,
                        propagation,
                        true,
                    );
                }
            }
            Instruction::AdDetach(_) => {}
            Instruction::Comment(_) => {}
            Instruction::CoroSplitMark { .. } => {}
            Instruction::CoroSuspend { .. } => {}
            Instruction::CoroResume { .. } => {}
            Instruction::CoroRegister { value, .. } => {
                Self::collect_node_direct_refs(
                    value,
                    direct_refs,
                    ancestor_direct_refs,
                    propagation,
                    true,
                );
            }
        }
    }

    pub fn propagate(tree: &ScopeTree) -> VariablePropagation {
        assert_eq!(tree.root().blocks.len(), 1);
        let mut prop = VariablePropagation {
            indices: HashMap::new(),
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

struct VariableDemoter<'a> {
    scope_tree: &'a ScopeTree,
    propagation: &'a VariablePropagation,
}

struct VariableDemotion {
    relocation: HashMap<*const BasicBlock, HashSet<NodeRef>>,
}

// Implementation of Step 3: demote variables to proper places.
impl<'a> VariableDemoter<'a> {
    fn new(scope_tree: &'a ScopeTree, propagation: &'a VariablePropagation) -> Self {
        Self {
            scope_tree,
            propagation,
        }
    }

    // In each block, demote a variable definition here if the following conditions are met:
    //    a. the variable has not been demoted by the node's ancestor (i.e., in the `remaining` set), and
    //       b.1 the variable is found propagated to more than one child node, or
    //       b.2 the variable is found directly referenced in the node.
    fn relocate_in_block(
        &self,
        tree_block: &'a ScopeTreeBlock,
        demotion: &mut VariableDemotion,
        remaining: &mut HashSet<NodeRef>,
    ) {
        let collection = self
            .propagation
            .collections
            .get(&tree_block.block.as_ptr())
            .unwrap();
        // a + b.1
        let mut demoted: HashSet<_> = remaining
            .intersection(&collection.direct_refs)
            .map(|node| node.clone())
            .collect();
        // a + b.2
        let mut references = HashSet::new();
        for &child_tree_node in tree_block.children.iter() {
            let child_tree_node = &self.scope_tree.nodes[child_tree_node];
            for child_tree_block in child_tree_node.blocks.iter() {
                let child_collection = self
                    .propagation
                    .collections
                    .get(&child_tree_block.block.as_ptr())
                    .unwrap();
                macro_rules! process_set {
                    ($set: expr) => {
                        for node in $set.iter() {
                            if remaining.contains(node)            // not demoted by the ancestors
                                && !demoted.contains(node)         // not demoted by the current node
                                && !references.insert(node.clone())// more than one occurrence
                            {
                                demoted.insert(node.clone());
                            }
                        }
                    };
                }
                process_set!(child_collection.direct_refs);
                process_set!(child_collection.propagated_refs);
            }
        }
        // remove the demoted variables from remaining
        remaining.retain(|node| !demoted.contains(node));
        // record the demoted variables
        demotion
            .relocation
            .insert(tree_block.block.as_ptr(), demoted);
        // recursively demote in children
        for &child_tree_node in tree_block.children.iter() {
            let child_tree_node = &self.scope_tree.nodes[child_tree_node];
            for child_tree_block in child_tree_node.blocks.iter() {
                self.relocate_in_block(child_tree_block, demotion, remaining);
            }
        }
    }

    fn process(&self, module: &Module) -> VariableDemotion {
        let mut demotion = VariableDemotion {
            relocation: HashMap::new(),
        };
        let entry = module.entry.as_ptr();
        let entry_collection = self.propagation.collections.get(&entry).unwrap();
        let mut remaining = HashSet::new();
        remaining.extend(entry_collection.direct_refs.iter());
        remaining.extend(entry_collection.propagated_refs.iter());
        self.relocate_in_block(
            &self.scope_tree.root().blocks[0],
            &mut demotion,
            &mut remaining,
        );
        assert!(remaining.is_empty(), "some variables are not demoted");
        demotion
    }
}

impl DemoteLocals {
    fn find_unused_locals_in_block(
        tree: &ScopeTree,
        tree_block: &ScopeTreeBlock,
        referenced: &HashMap<NodeRef, usize>,
        unused: &mut HashSet<NodeRef>,
    ) {
        unused.extend(
            tree_block
                .block
                .iter()
                .filter(|node| node.is_local() && !referenced.contains_key(node)),
        );
        for &i in tree_block.children.iter() {
            for child in tree.nodes[i].blocks.iter() {
                Self::find_unused_locals_in_block(tree, child, referenced, unused);
            }
        }
    }

    fn remove_unused_locals(module: &Module, tree: &ScopeTree, p: &VariablePropagation) {
        assert_eq!(tree.root().blocks.len(), 1);
        let entry = &module.entry;
        assert_eq!(tree.root().blocks[0].block.as_ptr(), entry.as_ptr());
        let referenced = &p.indices;
        let mut unused = HashSet::new();
        Self::find_unused_locals_in_block(&tree, &tree.root().blocks[0], referenced, &mut unused);
        for node in unused.iter() {
            node.remove();
        }
    }

    fn demote_directly_referenced_locals_in_node(node: &NodeRef, locals: &mut HashSet<NodeRef>) {
        macro_rules! demote {
            ($local: expr) => {
                if (locals.remove($local)) {
                    node.insert_before_self($local.clone());
                }
            };
        }
        match node.get().instruction.as_ref() {
            Instruction::Local { .. } => panic!("should be removed"),
            Instruction::Update { var, .. } => {
                demote!(var);
            }
            Instruction::Call(_, args) => {
                for arg in args.iter() {
                    demote!(arg);
                }
            }
            Instruction::RayQuery { ray_query, .. } => {
                demote!(ray_query);
            }
            Instruction::Print { args, .. } => {
                for arg in args.iter() {
                    demote!(arg);
                }
            }
            Instruction::CoroRegister { value, .. } => {
                demote!(value);
            }
            _ => {}
        }
    }

    fn demote_locals_in_block(
        tree: &ScopeTree,
        tree_block: &ScopeTreeBlock,
        propagation: &VariablePropagation,
        demotion: &VariableDemotion,
    ) {
        let bb = &tree_block.block;
        // records the variables that are yet to be demoted
        let mut locals = demotion.relocation.get(&bb.as_ptr()).unwrap().clone();
        // demote directly referenced locals by the nodes in the block
        for node in bb.iter() {
            Self::demote_directly_referenced_locals_in_node(&node, &mut locals);
        }
        // still some locals are not demoted, which means they are demoted here
        // because of being propagated to more than one child node, so we have
        // to check the nested blocks in the block
        if !locals.is_empty() {
            for &i in tree_block.children.iter() {
                let child_tree_node = &tree.nodes[i];
                // demote the variables before the child node if any of its
                // nested blocks references the variable
                let mut demoted_before_child: HashSet<NodeRef> = HashSet::new();
                for child in child_tree_node.blocks.iter() {
                    let prop = propagation.collections.get(&child.block.as_ptr()).unwrap();
                    let referenced: HashSet<_> = prop
                        .direct_refs
                        .union(&prop.propagated_refs)
                        .filter(|node| locals.contains(node))
                        .collect();
                    locals.retain(|node| !referenced.contains(node));
                    demoted_before_child.extend(referenced);
                }
                // move the variable definitions before the child node, remember to keep the order
                let node = child_tree_node.node;
                let mut demoted_before_child: Vec<_> = demoted_before_child.into_iter().collect();
                demoted_before_child.sort_by_key(|node| *propagation.indices.get(node).unwrap());
                for &demoted in demoted_before_child.iter() {
                    node.insert_before_self(demoted);
                }
            }
        }
        assert!(locals.is_empty(), "some variables are not demoted");
    }

    fn demote_locals(
        module: &Module,
        tree: &ScopeTree,
        propagation: &VariablePropagation,
        demotion: &VariableDemotion,
    ) {
        // 1. Move all variable definitions out of the module
        for (node, _) in propagation.indices.iter() {
            node.remove();
        }
        // 2. Move variable definitions to proper places, where
        //   a. the variable is found to be demoted in the current block, and
        //   b. the references of the variable are right after the definition.
        // Also, we should preserve the order of the variable definitions so
        // the kernel compilation cache can work.
        let entry = &module.entry;
        assert_eq!(tree.root().blocks.len(), 1);
        assert_eq!(tree.root().blocks[0].block.as_ptr(), entry.as_ptr());
        let tree_block = &tree.root().blocks[0];
        Self::demote_locals_in_block(tree, tree_block, propagation, demotion);
    }
}

impl Transform for DemoteLocals {
    fn transform_module(&self, module: Module) -> Module {
        let scope_tree = ScopeTree::from(&module);
        let propagation = VariablePropagator::propagate(&scope_tree);
        println!(
            "DemoteLocals: before demotion:\n{}",
            dump_ir_human_readable(&module)
        );
        // remove unused locals
        Self::remove_unused_locals(&module, &scope_tree, &propagation);
        println!(
            "DemoteLocals: after removing unused locals:\n{}",
            dump_ir_human_readable(&module)
        );
        // demote locals
        let demotion = VariableDemoter::new(&scope_tree, &propagation).process(&module);
        Self::demote_locals(&module, &scope_tree, &propagation, &demotion);
        println!(
            "DemoteLocals: after demotion:\n{}",
            dump_ir_human_readable(&module)
        );
        module
    }
}
