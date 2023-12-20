// This file implement def-use analysis for coroutine scopes.
// For each coroutine scope, at each instruction, the analysis will record the set of
// definitions that are alive at that instruction. If the instruction uses some value
// that is not alive at that instruction, then the value must be defined other places
// so we would have to pass the value through the coroutine frame.
// The analysis tries to perform member-wise analysis, so the values that have to be
// passed through the coroutine frame are minimized.

use crate::ir::NodeRef;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

// The access tree records the accessed members of a value, where accessed children are
// individual child nodes of the parent node. Specially, if a node has no children, then
// it is a leaf node and its value is accessed as a whole.

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccessNodeRef(pub usize);

#[derive(Debug, Clone)]
pub(crate) struct AccessNode {
    children: HashMap<usize, AccessNodeRef>,
}

#[derive(Debug, Clone)]
pub(crate) struct AccessTree {
    nodes: HashMap<NodeRef, AccessNodeRef>,
    _storage: Vec<AccessNode>,
}

impl AccessTree {
    fn get(&self, node: AccessNodeRef) -> &AccessNode {
        &self._storage[node.0]
    }

    fn get_mut(&mut self, node: AccessNodeRef) -> &mut AccessNode {
        &mut self._storage[node.0]
    }

    // insert a new node into the tree
    fn insert(&mut self, node: NodeRef, access_chain: &[usize]) {
        let mut parent_is_new = false;
        let root = self
            .nodes
            .entry(node)
            .or_insert_with(|| {
                parent_is_new = true;
                let index = self._storage.len();
                self._storage.push(AccessNode {
                    children: HashMap::new(),
                });
                AccessNodeRef(index)
            })
            .clone();
        let mut access_node_ref = root;
        for i in access_chain {
            if self.get(access_node_ref).children.is_empty() && !parent_is_new {
                // the parent node is an existing leaf node (accessed as a whole), so
                // we don't need to insert a new node for it any more
                break;
            } else if let Some(&child) = self.get(access_node_ref).children.get(i) {
                // the child node already exists, so we just need to go into it
                access_node_ref = child;
            } else {
                // the child node does not exist, so we need to insert a new node
                // into it before going into it
                let new_node = AccessNodeRef(self._storage.len());
                self._storage.push(AccessNode {
                    children: HashMap::new(),
                });
                self.get_mut(access_node_ref)
                    .children
                    .insert(i, new_node.clone());
                access_node_ref = new_node;
            }
        }
    }

    // check if a node is accessed as a whole
    fn all(&self, node: NodeRef, access_chain: &[usize]) -> bool {
        todo!()
    }

    // check if a node or any of its children is accessed
    fn any(&self, node: NodeRef, access_chain: &[usize]) -> bool {
        todo!()
    }

    // intersect two access trees
    fn intersect(&self, other: &Self) -> Self {
        todo!()
    }
}

// In the structured IR, an instruction dominates all successive instructions in the
// same basic block. So the def-use analysis can be performed in a single pass:
// 1. For simple instructions, just propagate the defs and uses.
// 2. For branch instructions (e.g. `if` and `switch`), each branch is a new basic
//    block. We copy the defs and uses from the current basic block to the new basic
//    blocks, and then perform the analysis on each branch. At the end, we merge the
//    defs and uses from all branches by taking the intersection.
// 3. For loop instructions, only the `do-while` loops are remained after the control
//    flow canonicalization pass, which can be viewed as a single basic block and no
//    special handling is needed.
// 4. For calls into custom callables, we incorporate the argument usage analysis to
//    decide which arguments are used in the callee and which are not. Note: we do not
//    consider if the callee defines the arguments by reference for simplicity. This
//    produces a conservative results and might be fixed in the future by inlining the
//    callee during analysis.

struct CoroScopeDefUse {
    external_uses: AccessTree,
    internal_defs: HashMap<u32, AccessTree>, // suspend -> defs
}

struct CoroDefUseAnalysis {
    scopes: HashMap<NodeRef, AccessTree>,
}
