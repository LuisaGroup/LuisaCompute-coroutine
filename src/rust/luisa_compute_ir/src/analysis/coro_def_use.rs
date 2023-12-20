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

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccessTreeNodeRef<'a> {
    tree: &'a AccessTree,
    node: AccessNodeRef,
}

impl<'a> AccessTreeNodeRef<'a> {
    fn new(tree: &'a AccessTree, node: AccessNodeRef) -> Self {
        Self { tree, node }
    }

    fn get(&self) -> &AccessNode {
        self.tree.get(self.node)
    }

    fn child(&self, i: usize) -> Option<Self> {
        self.get()
            .children
            .get(&i)
            .map(|&n| Self::new(self.tree, n))
    }

    fn has_children(&self) -> bool {
        !self.get().children.is_empty()
    }

    fn has_child(&self, i: usize) -> bool {
        let children = &self.get().children;
        children.is_empty() || children.contains_key(&i)
    }

    fn children(&'a self) -> impl Iterator<Item = (usize, Self)> {
        self.get()
            .children
            .iter()
            .map(move |(&i, &n)| (i, Self::new(self.tree, n)))
    }
}

impl AccessTree {
    fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            _storage: Vec::new(),
        }
    }

    fn get(&self, node: AccessNodeRef) -> &AccessNode {
        &self._storage[node.0]
    }

    fn get_mut(&mut self, node: AccessNodeRef) -> &mut AccessNode {
        &mut self._storage[node.0]
    }

    fn add_node(&mut self) -> AccessNodeRef {
        let index = self._storage.len();
        self._storage.push(AccessNode {
            children: HashMap::new(),
        });
        AccessNodeRef(index)
    }

    fn push(&mut self, node: AccessNode) -> AccessNodeRef {
        let index = self._storage.len();
        self._storage.push(node);
        AccessNodeRef(index)
    }

    // insert a new node into the tree
    fn insert(&mut self, node: NodeRef, access_chain: &[usize]) {
        let mut parent_is_new = false;
        let root = if let Some(root) = self.nodes.get(&node) {
            root.clone()
        } else {
            let new_node = self.add_node();
            self.nodes.insert(node, new_node.clone());
            parent_is_new = true;
            new_node
        };
        let mut access_node_ref = root;
        for i in access_chain {
            if self.get(access_node_ref).children.is_empty() && !parent_is_new {
                // the parent node is an existing leaf node (accessed as a whole), so
                // we don't need to insert a new node for it any more
                break;
            }
            if let Some(child) = self.get(access_node_ref).children.get(i).cloned() {
                // the child node already exists, so we just need to go into it
                access_node_ref = child;
                parent_is_new = false;
            } else {
                // the child node does not exist, so we need to insert a new node
                // into it before going into it
                let new_node = self.add_node();
                self.get_mut(access_node_ref)
                    .children
                    .insert(*i, new_node.clone());
                parent_is_new = true;
                access_node_ref = new_node;
            }
        }
        // mark that the node is accessed as a whole
        self.get_mut(access_node_ref).children.clear();
    }

    // check if a node is accessed with the given access chain
    fn contains(&self, node: NodeRef, access_chain: &[usize]) -> bool {
        if let Some(access_node_ref) = self.nodes.get(&node).cloned() {
            let mut access_node_ref = access_node_ref;
            for i in access_chain {
                if self.get(access_node_ref).children.is_empty() {
                    // The node is accessed as a whole, so any further access to its children is contained.
                    return true;
                } else if let Some(child) = self.get(access_node_ref).children.get(i).cloned() {
                    // go into the child node if it exists
                    access_node_ref = child;
                } else {
                    // the child node does not exist, so the access is not contained
                    return false;
                }
            }
            self.get(access_node_ref).children.is_empty()
        } else {
            false
        }
    }

    fn _clone(&mut self, a: AccessTreeNodeRef) -> AccessNodeRef {
        let new_node = AccessNode {
            children: a
                .children()
                .map(|(i, node)| (i, self._clone(node)))
                .collect(),
        };
        self.push(new_node)
    }

    fn _merge(&mut self, a: AccessTreeNodeRef, b: AccessTreeNodeRef) -> Option<AccessNodeRef> {
        if !a.has_children() {
            // a is accessed as a whole (a is a full set), then the intersection is b
            Some(self._clone(b))
        } else if !b.has_children() {
            // b is accessed as a whole (b is a full set), then the intersection is a
            Some(self._clone(a))
        } else {
            // find the common children as the intersection
            let children: HashMap<_, _> = a
                .children()
                .filter_map(|(i, a)| b.child(i).and_then(|b| self._merge(a, b)).map(|n| (i, n)))
                .collect();
            if children.is_empty() {
                // no common children, so the intersection is empty
                None
            } else {
                Some(self.push(AccessNode { children }))
            }
        }
    }

    // intersect two access trees
    fn intersect(&self, other: &Self) -> Self {
        let mut new_tree = Self::new();
        for common_node in self.nodes.keys().filter(|k| other.nodes.contains_key(k)) {
            if let Some(merged) = new_tree._merge(
                AccessTreeNodeRef::new(self, self.nodes[common_node]),
                AccessTreeNodeRef::new(other, other.nodes[common_node]),
            ) {
                new_tree.nodes.insert(common_node.clone(), merged);
            }
        }
        new_tree
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
