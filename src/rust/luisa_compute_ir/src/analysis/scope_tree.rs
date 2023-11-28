// This file implements the scope tree analysis, which traverse the IR and construct a tree
// where each node is a instruction that contains nested basic blocks.
// Note: this analysis only works on a single module without recurse into its callees.

use crate::ir::{BasicBlock, Instruction, Module, ModulePools, NodeRef, INVALID_REF};
use crate::{CArc, Pooled};

pub struct ScopeTreeNode {
    node: NodeRef, // INVALID_REF for the root node
    blocks: Vec<ScopeTreeBlock>,
}

pub struct ScopeTreeBlock {
    block: Pooled<BasicBlock>,
    children: Vec<usize>, // indices into the tree nodes
}

pub struct ScopeTree {
    pools: CArc<ModulePools>,
    nodes: Vec<ScopeTreeNode>,
}

struct ScopeTreeBuilder;

impl ScopeTreeBuilder {
    fn process_basic_block(bb: &Pooled<BasicBlock>, tree: &mut ScopeTree, parent: usize) {
        let block = tree.nodes[parent].blocks.len();
        tree.nodes[parent].blocks.push(ScopeTreeBlock {
            block: bb.clone(),
            children: Vec::new(),
        });
        macro_rules! add_child {
            ($node: expr) => {{
                let child = tree.nodes.len();
                tree.nodes.push(ScopeTreeNode {
                    node: $node,
                    blocks: Vec::new(),
                });
                tree.nodes[parent].blocks[block].children.push(child);
                child
            }};
        }
        for node in bb.iter() {
            let instr = node.get().instruction.as_ref();
            match instr {
                Instruction::Loop { body, .. } => {
                    let child = add_child!(node.clone());
                    Self::process_basic_block(body, tree, child);
                }
                Instruction::GenericLoop {
                    prepare,
                    body,
                    update,
                    ..
                } => {
                    let child = add_child!(node.clone());
                    Self::process_basic_block(prepare, tree, child);
                    Self::process_basic_block(body, tree, child);
                    Self::process_basic_block(update, tree, child);
                }
                Instruction::If {
                    true_branch,
                    false_branch,
                    ..
                } => {
                    let child = add_child!(node.clone());
                    Self::process_basic_block(true_branch, tree, child);
                    Self::process_basic_block(false_branch, tree, child);
                }
                Instruction::Switch { cases, default, .. } => {
                    let child = add_child!(node.clone());
                    for case in cases.iter() {
                        Self::process_basic_block(&case.block, tree, child);
                    }
                    Self::process_basic_block(default, tree, child);
                }
                Instruction::AdScope { body, .. } => {
                    let child = add_child!(node.clone());
                    Self::process_basic_block(body, tree, child);
                }
                Instruction::RayQuery {
                    on_triangle_hit,
                    on_procedural_hit,
                    ..
                } => {
                    let child = add_child!(node.clone());
                    Self::process_basic_block(on_triangle_hit, tree, child);
                    Self::process_basic_block(on_procedural_hit, tree, child);
                }
                Instruction::AdDetach(body) => {
                    let child = add_child!(node.clone());
                    Self::process_basic_block(body, tree, child);
                }
                _ => {}
            }
        }
    }

    pub fn build(module: &Pooled<Module>) -> ScopeTree {
        let mut tree = ScopeTree {
            pools: module.pools.clone(),
            nodes: vec![ScopeTreeNode {
                node: INVALID_REF,
                blocks: vec![],
            }],
        };
        Self::process_basic_block(&module.entry, &mut tree, 0);
        tree
    }
}

impl ScopeTree {
    pub fn from(module: &Pooled<Module>) -> ScopeTree {
        ScopeTreeBuilder::build(module)
    }

    pub fn root(&self) -> &ScopeTreeNode {
        &self.nodes[0]
    }
}
