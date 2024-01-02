// This file implements the local variable demotion transform.
//
// This transform demotes local variables to its lowest possible scope, i.e., to right
// before the lowest common ancestor of all its references, in the following steps:
// 0. Normalize the initialization of local variables:
//    a. uniform initializers are hoisted to top
//       - ZeroInitializer
//       - Const
//       - Uniform
//       - Argument (by_value = true)
//       - One/Zero
//    b. non-uniform initializers are split into a zero-initializer and an update.
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

use crate::analysis::replayable_values::ReplayableValueAnalysis;
use crate::analysis::scope_tree::{ScopeTree, ScopeTreeBlock};
use crate::display::DisplayIR;
use crate::ir::{BasicBlock, Func, Instruction, IrBuilder, Module, NodeRef};
use crate::transform::remove_phi::RemovePhi;
use crate::transform::Transform;
use crate::CBoxedSlice;
use std::collections::{HashMap, HashSet};

pub struct DemoteLocals;

struct InitializationNormalizer;

// Implementation of Step 0: normalize the initialization of local variables.
impl InitializationNormalizer {
    fn normalize_non_uniform_inits_in_block(
        module: &Module,
        block: &BasicBlock,
        uniform_analysis: &mut ReplayableValueAnalysis,
    ) {
        for node in block.iter() {
            match node.get().instruction.as_ref() {
                Instruction::Local { init } => {
                    if !uniform_analysis.detect(init.clone()) {
                        let mut builder = IrBuilder::new(module.pools.clone());
                        // insert a zero initializer before the local variable
                        builder.set_insert_point(node.get().prev);
                        let local = builder.local_zero_init(node.type_().clone());
                        // replace the local variable with the zero initializer
                        local.remove();
                        node.replace_with(local.get());
                        // insert the update after the local variable
                        builder.set_insert_point(node.clone());
                        builder.update(node, init.clone());
                    }
                }
                Instruction::Loop { body, .. } => {
                    Self::normalize_non_uniform_inits_in_block(module, body, uniform_analysis);
                }
                Instruction::GenericLoop {
                    prepare,
                    body,
                    update,
                    ..
                } => {
                    Self::normalize_non_uniform_inits_in_block(module, prepare, uniform_analysis);
                    Self::normalize_non_uniform_inits_in_block(module, body, uniform_analysis);
                    Self::normalize_non_uniform_inits_in_block(module, update, uniform_analysis);
                }
                Instruction::If {
                    true_branch,
                    false_branch,
                    ..
                } => {
                    Self::normalize_non_uniform_inits_in_block(
                        module,
                        true_branch,
                        uniform_analysis,
                    );
                    Self::normalize_non_uniform_inits_in_block(
                        module,
                        false_branch,
                        uniform_analysis,
                    );
                }
                Instruction::Switch { cases, default, .. } => {
                    for case in cases.iter() {
                        Self::normalize_non_uniform_inits_in_block(
                            module,
                            &case.block,
                            uniform_analysis,
                        );
                    }
                    Self::normalize_non_uniform_inits_in_block(module, default, uniform_analysis);
                }
                Instruction::AdScope { body, .. } => {
                    Self::normalize_non_uniform_inits_in_block(module, body, uniform_analysis);
                }
                Instruction::RayQuery {
                    on_triangle_hit,
                    on_procedural_hit,
                    ..
                } => {
                    Self::normalize_non_uniform_inits_in_block(
                        module,
                        on_triangle_hit,
                        uniform_analysis,
                    );
                    Self::normalize_non_uniform_inits_in_block(
                        module,
                        on_procedural_hit,
                        uniform_analysis,
                    );
                }
                Instruction::AdDetach(body) => {
                    Self::normalize_non_uniform_inits_in_block(module, body, uniform_analysis);
                }
                _ => {}
            }
        }
    }

    fn collect_const_inits_in_block(block: &BasicBlock, inits: &mut Vec<NodeRef>) {
        for node in block.iter() {
            match node.get().instruction.as_ref() {
                Instruction::Local { init } => match init.get().instruction.as_ref() {
                    Instruction::Const(_) => {
                        inits.push(init.clone());
                    }
                    Instruction::Call(func, args) => match func {
                        Func::ZeroInitializer
                        | Func::DispatchId
                        | Func::ThreadId
                        | Func::BlockId
                        | Func::WarpLaneId
                        | Func::DispatchSize
                        | Func::WarpSize
                        | Func::CoroId
                        | Func::CoroToken => {
                            inits.push(init.clone());
                        }
                        _ => {}
                    },
                    _ => {}
                },
                Instruction::Loop { body, .. } => {
                    Self::collect_const_inits_in_block(body, inits);
                }
                Instruction::GenericLoop {
                    prepare,
                    body,
                    update,
                    ..
                } => {
                    Self::collect_const_inits_in_block(prepare, inits);
                    Self::collect_const_inits_in_block(body, inits);
                    Self::collect_const_inits_in_block(update, inits);
                }
                Instruction::If {
                    true_branch,
                    false_branch,
                    ..
                } => {
                    Self::collect_const_inits_in_block(true_branch, inits);
                    Self::collect_const_inits_in_block(false_branch, inits);
                }
                Instruction::Switch { cases, default, .. } => {
                    for case in cases.iter() {
                        Self::collect_const_inits_in_block(&case.block, inits);
                    }
                    Self::collect_const_inits_in_block(default, inits);
                }
                Instruction::AdScope { body, .. } => {
                    Self::collect_const_inits_in_block(body, inits);
                }
                Instruction::RayQuery {
                    on_triangle_hit,
                    on_procedural_hit,
                    ..
                } => {
                    Self::collect_const_inits_in_block(on_triangle_hit, inits);
                    Self::collect_const_inits_in_block(on_procedural_hit, inits);
                }
                Instruction::AdDetach(body) => {
                    Self::collect_const_inits_in_block(body, inits);
                }
                _ => {}
            }
        }
    }

    fn normalize(module: &Module) {
        let mut uniform_analysis = ReplayableValueAnalysis::new_with_module(true, module);
        Self::normalize_non_uniform_inits_in_block(module, &module.entry, &mut uniform_analysis);
        let mut uniform_inits = Vec::new();
        Self::collect_const_inits_in_block(&module.entry, &mut uniform_inits);
        // uniquify the initializers
        let mut unique_inits = HashMap::new();
        for (i, node) in uniform_inits.iter().enumerate() {
            unique_inits.entry(node).or_insert(i);
        }
        let mut uniform_inits: Vec<_> = unique_inits.keys().cloned().cloned().collect();
        uniform_inits.sort_by_key(|node| unique_inits.get(node).unwrap());
        for init in uniform_inits.iter() {
            init.remove();
        }
        for &init in uniform_inits.iter().rev() {
            module.entry.first.insert_after_self(init);
        }
        // TODO: merge the initializers if possible
    }
}

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
        propagation: &mut VariablePropagation,
    ) {
        let mut collection = VariableCollection {
            direct_refs: HashSet::new(),
            propagated_refs: HashSet::new(),
        };
        // collect direct references
        for node in tree_block.block.iter() {
            Self::collect_node_direct_refs(&node, &mut collection.direct_refs, propagation, false);
        }
        // propagate references from children
        if !tree_block.children.is_empty() {
            // traverse children
            for &i in tree_block.children.iter() {
                let mut builder = IrBuilder::new(tree.pools.clone());
                builder.set_insert_point(tree.nodes[i].node.get().prev);
                builder.comment(CBoxedSlice::from(
                    format!("ScopeTree Node {}", i).to_string(),
                ));
                for child in tree.nodes[i].blocks.iter() {
                    Self::propagate_block(tree, child, propagation);
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
        propagation: &mut VariablePropagation,
        is_referenced: bool,
    ) {
        macro_rules! check_not_local {
            ($node: expr) => {
                assert!(
                    !($node.valid() && $node.is_local()),
                    "local variable not loaded"
                );
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
                    direct_refs.insert(node.clone());
                    let index = propagation.indices.len();
                    propagation.indices.entry(node.clone()).or_insert(index);
                } else {
                    check_not_local!(init);
                }
            }
            Instruction::Argument { .. } => {}
            Instruction::UserData(_) => {}
            Instruction::Invalid => {}
            Instruction::Const(_) => {}
            Instruction::Update { var, value } => {
                Self::collect_node_direct_refs(var, direct_refs, propagation, true);
                check_not_local!(value);
            }
            Instruction::Call(_, args) => {
                for arg in args.iter() {
                    Self::collect_node_direct_refs(arg, direct_refs, propagation, true);
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
                Self::collect_node_direct_refs(ray_query, direct_refs, propagation, true);
            }
            Instruction::Print { args, .. } => {
                for arg in args.iter() {
                    Self::collect_node_direct_refs(arg, direct_refs, propagation, true);
                }
            }
            Instruction::AdDetach(_) => {}
            Instruction::Comment(_) => {}
            Instruction::CoroSplitMark { .. } => {}
            Instruction::CoroSuspend { .. } => {}
            Instruction::CoroResume { .. } => {}
            Instruction::CoroRegister { value, .. } => {
                Self::collect_node_direct_refs(value, direct_refs, propagation, true);
            }
        }
    }

    pub fn propagate(tree: &ScopeTree) -> VariablePropagation {
        assert_eq!(tree.root().blocks.len(), 1);
        let mut prop = VariablePropagation {
            indices: HashMap::new(),
            collections: HashMap::new(),
        };
        Self::propagate_block(&tree, &tree.root().blocks[0], &mut prop);
        prop
    }
}

struct VariableRelocator<'a> {
    scope_tree: &'a ScopeTree,
    propagation: &'a VariablePropagation,
}

struct VariableRelocation {
    relocation: HashMap<*const BasicBlock, HashSet<NodeRef>>,
}

// Implementation of Step 3: demote variables to proper places.
impl<'a> VariableRelocator<'a> {
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
        relocation: &mut VariableRelocation,
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
        relocation
            .relocation
            .insert(tree_block.block.as_ptr(), demoted);
        // recursively demote in children
        for &child_tree_node in tree_block.children.iter() {
            let child_tree_node = &self.scope_tree.nodes[child_tree_node];
            for child_tree_block in child_tree_node.blocks.iter() {
                self.relocate_in_block(child_tree_block, relocation, remaining);
            }
        }
    }

    fn process(&self, module: &Module) -> VariableRelocation {
        let mut relocation = VariableRelocation {
            relocation: HashMap::new(),
        };
        let entry = module.entry.as_ptr();
        let entry_collection = self.propagation.collections.get(&entry).unwrap();
        let mut remaining = HashSet::new();
        remaining.extend(entry_collection.direct_refs.iter());
        remaining.extend(entry_collection.propagated_refs.iter());
        self.relocate_in_block(
            &self.scope_tree.root().blocks[0],
            &mut relocation,
            &mut remaining,
        );
        assert!(remaining.is_empty(), "some variables are not demoted");
        relocation
    }
}

impl DemoteLocals {
    fn collect_locals_in_block(
        tree: &ScopeTree,
        tree_block: &ScopeTreeBlock,
        locals: &mut HashSet<NodeRef>,
    ) {
        locals.extend(tree_block.block.iter().filter(|node| node.is_local()));
        for &i in tree_block.children.iter() {
            let child_tree_node = &tree.nodes[i];
            for child_tree_block in child_tree_node.blocks.iter() {
                Self::collect_locals_in_block(tree, child_tree_block, locals);
            }
        }
    }

    fn remove_locals(module: &Module, tree: &ScopeTree) {
        assert_eq!(tree.root().blocks.len(), 1);
        let entry = &module.entry;
        assert_eq!(tree.root().blocks[0].block.as_ptr(), entry.as_ptr());
        let mut locals = HashSet::new();
        Self::collect_locals_in_block(&tree, &tree.root().blocks[0], &mut locals);
        for node in locals.iter() {
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
        demotion: &VariableRelocation,
    ) {
        let bb = &tree_block.block;
        // records the variables that are yet to be demoted
        let mut locals = demotion.relocation.get(&bb.as_ptr()).unwrap().clone();
        // let first = bb.first.get().next;
        // for &local in locals.iter() {
        //     first.insert_before_self(local);
        // }
        // return;
        // demote directly referenced locals by the nodes in the block
        let nested_block_nodes: HashMap<_, _> = tree_block
            .children
            .iter()
            .map(|&i| (tree.nodes[i].node.clone(), i))
            .collect();
        for node in bb.iter() {
            Self::demote_directly_referenced_locals_in_node(&node, &mut locals);
            if let Some(&i) = nested_block_nodes.get(&node) {
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
        // recursively demote in children
        for &i in tree_block.children.iter() {
            let child_tree_node = &tree.nodes[i];
            for child_tree_block in child_tree_node.blocks.iter() {
                Self::demote_locals_in_block(tree, child_tree_block, propagation, demotion);
            }
        }
    }

    fn demote_locals(
        module: &Module,
        tree: &ScopeTree,
        propagation: &VariablePropagation,
        relocation: &VariableRelocation,
    ) {
        // 1. Remove all local variable definitions.
        Self::remove_locals(module, tree);
        // 2. Re-insert variable definitions to proper places, where
        //   a. the variable is found to be demoted in the current block, and
        //   b. the references of the variable are right after the definition.
        // Also, we should preserve the order of the variable definitions so
        // the kernel compilation cache can work.
        let entry = &module.entry;
        assert_eq!(tree.root().blocks.len(), 1);
        assert_eq!(tree.root().blocks[0].block.as_ptr(), entry.as_ptr());
        let tree_block = &tree.root().blocks[0];
        Self::demote_locals_in_block(tree, tree_block, propagation, relocation);
    }
}

impl Transform for DemoteLocals {
    fn transform_module(&self, module: Module) -> Module {
        let module = RemovePhi.transform_module(module);
        println!("{:-^40}", " After RemovePhi ");
        println!("{}", DisplayIR::new().display_ir(&module));

        InitializationNormalizer::normalize(&module);
        let scope_tree = ScopeTree::from(&module);
        let propagation = VariablePropagator::propagate(&scope_tree);
        // println!(
        //     "DemoteLocals: before demotion:\n{}",
        //     dump_ir_human_readable(&module)
        // );
        let demotion = VariableRelocator::new(&scope_tree, &propagation).process(&module);
        // demote locals
        Self::demote_locals(&module, &scope_tree, &propagation, &demotion);
        // println!(
        //     "DemoteLocals: after demotion:\n{}",
        //     dump_ir_human_readable(&module)
        // );
        module
    }
}
