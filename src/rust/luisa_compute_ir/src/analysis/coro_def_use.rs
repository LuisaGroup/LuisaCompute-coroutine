// This file implement def-use analysis for coroutine scopes.
// For each coroutine scope, at each instruction, the analysis will record the set of
// definitions that are alive at that instruction. If the instruction uses some value
// that is not alive at that instruction, then the value must be defined other places
// so we would have to pass the value through the coroutine frame.
// The analysis tries to perform member-wise analysis, so the values that have to be
// passed through the coroutine frame are minimized.

use crate::analysis::callable_arg_usages::CallableArgumentUsageAnalysis;
use crate::analysis::const_eval::ConstEval;
use crate::analysis::coro_graph::{CoroGraph, CoroInstrRef, CoroInstruction, CoroScopeRef};
use crate::analysis::replayable_values::ReplayableValueAnalysis;
use crate::ir::NodeRef;
use std::collections::{HashMap, HashSet};

// The access tree records the accessed members of a value, where accessed children are
// individual child nodes of the parent node. Specially, if a node has no children, then
// it is a leaf node and its value is accessed as a whole.

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccessNodeRef(pub usize);

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub(crate) enum AccessChainIndex {
    Static(i32),
    Dynamic(NodeRef),
}

impl From<i32> for AccessChainIndex {
    fn from(value: i32) -> Self {
        AccessChainIndex::Static(value)
    }
}

impl From<NodeRef> for AccessChainIndex {
    fn from(value: NodeRef) -> Self {
        AccessChainIndex::Dynamic(value)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AccessNode {
    children: HashMap<AccessChainIndex, AccessNodeRef>,
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

    fn child(&self, i: AccessChainIndex) -> Option<Self> {
        self.get()
            .children
            .get(&i)
            .map(|&n| Self::new(self.tree, n))
    }

    fn has_children(&self) -> bool {
        !self.get().children.is_empty()
    }

    fn has_child(&self, i: AccessChainIndex) -> bool {
        let children = &self.get().children;
        children.is_empty() || children.contains_key(&i)
    }

    fn children(&'a self) -> impl Iterator<Item = (AccessChainIndex, AccessTreeNodeRef<'a>)> {
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
    fn insert(&mut self, node: NodeRef, access_chain: &[AccessChainIndex]) {
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
    fn contains(&self, node: NodeRef, access_chain: &[AccessChainIndex]) -> bool {
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

    fn clone_node(&mut self, a: AccessTreeNodeRef) -> AccessNodeRef {
        let new_node = AccessNode {
            children: a
                .children()
                .map(|(i, node)| (i, self.clone_node(node)))
                .collect(),
        };
        self.push(new_node)
    }

    fn intersect_nodes(
        &mut self,
        a: AccessTreeNodeRef,
        b: AccessTreeNodeRef,
    ) -> Option<AccessNodeRef> {
        if !a.has_children() {
            // a is accessed as a whole (a is a full set), then the intersection is b
            Some(self.clone_node(b))
        } else if !b.has_children() {
            // b is accessed as a whole (b is a full set), then the intersection is a
            Some(self.clone_node(a))
        } else {
            // find the common children as the intersection
            let children: HashMap<_, _> = a
                .children()
                .filter_map(|(i, a)| {
                    b.child(i)
                        .and_then(|b| self.intersect_nodes(a, b))
                        .map(|n| (i, n))
                })
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
            if let Some(merged) = new_tree.intersect_nodes(
                AccessTreeNodeRef::new(self, self.nodes[common_node]),
                AccessTreeNodeRef::new(other, other.nodes[common_node]),
            ) {
                new_tree.nodes.insert(common_node.clone(), merged);
            }
        }
        new_tree
    }

    fn union_nodes(&mut self, a: AccessTreeNodeRef, b: AccessTreeNodeRef) -> AccessNodeRef {
        if !a.has_children() {
            // a is accessed as a whole (a is a full set), then the union is a
            self.clone_node(a)
        } else if !b.has_children() {
            // b is accessed as a whole (b is a full set), then the union is b
            self.clone_node(b)
        } else {
            let mut children = HashSet::new();
            for (i, _) in a.children() {
                children.insert(i);
            }
            for (i, _) in b.children() {
                children.insert(i);
            }
            let children = children
                .iter()
                .map(|&i| {
                    let child = match (a.child(i), b.child(i)) {
                        (Some(a), Some(b)) => self.union_nodes(a, b),
                        (Some(a), None) => self.clone_node(a),
                        (None, Some(b)) => self.clone_node(b),
                        (None, None) => unreachable!(),
                    };
                    (i, child)
                })
                .collect();
            self.push(AccessNode { children })
        }
    }

    fn union(&self, other: &Self) -> Self {
        let mut new_tree = Self::new();
        for common_node in self.nodes.keys().filter(|k| other.nodes.contains_key(k)) {
            let merged = new_tree.union_nodes(
                AccessTreeNodeRef::new(self, self.nodes[common_node]),
                AccessTreeNodeRef::new(other, other.nodes[common_node]),
            );
            new_tree.nodes.insert(common_node.clone(), merged);
        }
        for node in self.nodes.keys().filter(|k| !other.nodes.contains_key(k)) {
            new_tree
                .nodes
                .insert(node.clone(), self.nodes[node].clone());
        }
        for node in other.nodes.keys().filter(|k| !self.nodes.contains_key(k)) {
            new_tree
                .nodes
                .insert(node.clone(), other.nodes[node].clone());
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
// Handling of defs and uses in simple instructions:
// 1. If the node is a `Local`, then it *defines* this node.
// 2. If the node is an `Update`, then it *defines* the access chain of the updated node,
//    i.e., we walk through the GEP chain to `mark_def` the accessed members of the node.
// 3. If the node is a `Load`, then it *uses* the access chain of the loaded node, i.e.,
//    we walk through the GEP chain and `mark_use` the accessed members of the node. The
//    resulting node of the instruction *defines* the node itself (not the access chain).
// 4. Other should be simple enough to handle.

struct CoroScopeDefUse {
    // uses that are not dominated by defs in the current scope
    external_uses: AccessTree,
    // defs at the suspend point that can be safely localized in the current scope
    // note: the scope might define other values at the suspend point, but they are
    //       unsafe to localize because their defs do not dominate the suspend point
    internal_defs: HashMap<u32, AccessTree>,
}

impl CoroScopeDefUse {
    fn new() -> Self {
        Self {
            external_uses: AccessTree::new(),
            internal_defs: HashMap::new(),
        }
    }
}

struct CoroDefUseAnalysis<'a> {
    graph: &'a CoroGraph,
}

struct CoroDefUseHelperAnalyses {
    replayable: ReplayableValueAnalysis,
    callable_args: CallableArgumentUsageAnalysis,
    const_eval: ConstEval,
}

impl<'a> CoroDefUseAnalysis<'a> {
    fn analyze_branch_block(
        &self,
        block: &Vec<CoroInstrRef>,
        parent_defs: &AccessTree,
        def_use: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) -> AccessTree {
        let mut defs = parent_defs.clone();
        self.analyze_direct_block(block, &mut defs, def_use, helpers);
        defs
    }

    fn analyze_direct_block(
        &self,
        block: &Vec<CoroInstrRef>,
        defs: &mut AccessTree,
        def_use: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        for &instr in block.iter() {
            self.analyze_instr(self.graph.get_instr(instr), defs, def_use, helpers);
        }
    }

    fn partially_evaluate_access_chain(
        indices: &[NodeRef],
        helpers: &mut CoroDefUseHelperAnalyses,
    ) -> Vec<AccessChainIndex> {
        indices
            .iter()
            .map(|&i| {
                if let Some(value) = helpers.const_eval.eval(i).and_then(|v| v.try_get_i32()) {
                    AccessChainIndex::Static(value)
                } else {
                    AccessChainIndex::Dynamic(i)
                }
            })
            .collect()
    }

    fn mark_def(
        &self,
        node_ref: NodeRef,
        access_chain: &[NodeRef],
        defs: &mut AccessTree,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        let access_chain = Self::partially_evaluate_access_chain(access_chain, helpers);
        defs.insert(node_ref, access_chain.as_slice());
    }

    fn mark_use(
        &self,
        node_ref: NodeRef,
        access_chain: &[NodeRef],
        defs: &mut AccessTree,
        result: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        if helpers.replayable.detect(node_ref) {
            // the value is replayable, so we don't need to pass it through the frame
            return;
        }
        // otherwise, check if the value is dominated by defs in the current scope
        let access_chain = Self::partially_evaluate_access_chain(access_chain, helpers);
        if !defs.contains(node_ref, access_chain.as_slice()) {
            // the value is not dominated by defs in the current scope, so we need to
            // pass it through the frame
            result
                .external_uses
                .insert(node_ref, access_chain.as_slice());
        }
    }

    fn analyze_simple(
        &self,
        node: NodeRef,
        defs: &mut AccessTree,
        result: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        todo!()
    }

    fn analyze_instr(
        &self,
        instr: &CoroInstruction,
        defs: &mut AccessTree,
        result: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        match instr {
            CoroInstruction::Entry | CoroInstruction::EntryScope { .. } => unreachable!(),
            CoroInstruction::Simple(node) => {
                self.analyze_simple(*node, defs, result, helpers);
            }
            CoroInstruction::ConditionStackReplay { items } => {
                for item in items {
                    self.mark_def(item.node, &[], defs, helpers);
                }
            }
            CoroInstruction::MakeFirstFlag | CoroInstruction::ClearFirstFlag(_) => {
                // nothing to do as the first flag is always defined locally
            }
            CoroInstruction::SkipIfFirstFlag { body, .. } => {
                self.analyze_branch_block(body, defs, result, helpers);
            }
            CoroInstruction::Loop { body, cond } => {
                self.analyze_direct_block(body, defs, result, helpers);
                if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                    self.mark_use(cond.clone(), &[], defs, result, helpers);
                } else {
                    unreachable!()
                }
            }
            CoroInstruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                    self.mark_use(cond.clone(), &[], defs, result, helpers);
                } else {
                    unreachable!()
                }
                let true_defs = self.analyze_branch_block(true_branch, defs, result, helpers);
                let false_defs = self.analyze_branch_block(false_branch, defs, result, helpers);
                *defs = AccessTree::intersect(&true_defs, &false_defs);
            }
            CoroInstruction::Switch {
                cond,
                cases,
                default,
            } => {
                if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                    self.mark_use(cond.clone(), &[], defs, result, helpers);
                } else {
                    unreachable!()
                }
                let mut common_defs = self.analyze_branch_block(default, defs, result, helpers);
                for case in cases {
                    let case_defs = self.analyze_branch_block(&case.body, defs, result, helpers);
                    common_defs = AccessTree::intersect(&common_defs, &case_defs);
                }
                *defs = common_defs;
            }
            CoroInstruction::Suspend { token } => {
                // record the defs at the suspend point
                if let Some(defs) = result.internal_defs.get_mut(token) {
                    *defs = AccessTree::intersect(defs, defs);
                } else {
                    result.internal_defs.insert(*token, defs.clone());
                }
            }
            CoroInstruction::Terminate => {}
        }
    }
}

impl<'a> CoroDefUseAnalysis<'a> {
    pub fn new(graph: &'a CoroGraph) -> Self {
        Self { graph }
    }

    pub fn analyze(&self, scope_ref: CoroScopeRef) -> CoroScopeDefUse {
        let mut def_use = CoroScopeDefUse::new();
        let scope = self.graph.get_scope(scope_ref);
        self.analyze_direct_block(
            &scope.instructions,
            &mut AccessTree::new(),
            &mut def_use,
            &mut CoroDefUseHelperAnalyses {
                replayable: ReplayableValueAnalysis::new(false),
                callable_args: CallableArgumentUsageAnalysis::new(),
                const_eval: ConstEval::new(),
            },
        );
        def_use
    }
}
