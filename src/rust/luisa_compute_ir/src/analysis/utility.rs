use crate::analysis::const_eval::ConstEval;
use crate::display::DisplayIR;
use crate::ir::{Func, Instruction, NodeRef, Type};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

// Singleton pattern for DisplayIR
pub(crate) struct LazyDisplayIR {
    inner: Option<DisplayIR>,
}

impl LazyDisplayIR {
    const fn new() -> Self {
        Self { inner: None }
    }
}

impl LazyDisplayIR {
    pub(crate) fn get(&mut self) -> &mut DisplayIR {
        if self.inner.is_none() {
            self.inner = Some(DisplayIR::new());
        }
        self.inner.as_mut().unwrap()
    }
}

pub(crate) static mut DISPLAY_IR_DEBUG: LazyDisplayIR = LazyDisplayIR::new(); // for DEBUG

pub(crate) fn display_node_map2set(target: &HashMap<NodeRef, HashSet<i32>>) -> String {
    let output: Vec<_> = target
        .iter()
        .map(|(node_ref, number)| unsafe {
            let node = DISPLAY_IR_DEBUG.get().var_str(node_ref);
            let number = BTreeSet::from_iter(number.iter().cloned());
            let number = format!("{:?}", number);
            let output = format!("{}: {}", node, number);
            (node, output)
        })
        .collect();
    let output = BTreeMap::from_iter(output);
    let output: Vec<_> = output.iter().map(|(_, output)| output.as_str()).collect();
    let output = output.join(", ");
    let output = format!("{{{}}}", output);
    output
}

pub(crate) fn display_node_map(target: &HashMap<NodeRef, i32>) -> String {
    let output: Vec<_> = target
        .iter()
        .map(|(node_ref, number)| unsafe {
            let node = DISPLAY_IR_DEBUG.get().var_str(node_ref);
            let output = format!("{}: {}", node, number);
            (node, output)
        })
        .collect();
    let output = BTreeMap::from_iter(output);
    let output: Vec<_> = output.iter().map(|(_, output)| output.as_str()).collect();
    let output = output.join(", ");
    let output = format!("{{{}}}", output);
    output
}

pub(crate) fn display_node_set(target: &HashSet<NodeRef>) -> String {
    unsafe { DISPLAY_IR_DEBUG.get().vars_str(target) }
}

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
    pub children: HashMap<AccessChainIndex, AccessNodeRef>,
}

#[derive(Debug, Clone)]
pub(crate) struct AccessTree {
    pub nodes: HashMap<NodeRef, AccessNodeRef>,
    _storage: Vec<AccessNode>,
}

fn identical_access_tree_nodes(a: AccessTreeNodeRef, b: AccessTreeNodeRef) -> bool {
    let a_children = &a.get().children;
    let b_children = &b.get().children;
    a_children.len() == b_children.len()
        && a_children.keys().all(|k| b_children.contains_key(k))
        && a_children.iter().all(|(i, &a_node)| {
            let b_node = b_children[i];
            let a = AccessTreeNodeRef::new(a.tree, a_node);
            let b = AccessTreeNodeRef::new(b.tree, b_node);
            identical_access_tree_nodes(a, b)
        })
}

impl PartialEq for AccessTree {
    fn eq(&self, other: &Self) -> bool {
        self.nodes.len() == other.nodes.len()
            && self.nodes.keys().all(|k| other.nodes.contains_key(k))
            && self.nodes.iter().all(|(node, &a)| {
                let b = other.nodes[node];
                let a = AccessTreeNodeRef::new(self, a);
                let b = AccessTreeNodeRef::new(other, b);
                identical_access_tree_nodes(a, b)
            })
    }
}

impl Eq for AccessTree {}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccessTreeNodeRef<'a> {
    tree: &'a AccessTree,
    node: AccessNodeRef,
}

impl<'a> AccessTreeNodeRef<'a> {
    pub fn new(tree: &'a AccessTree, node: AccessNodeRef) -> Self {
        Self { tree, node }
    }

    pub fn get(&self) -> &AccessNode {
        self.tree.get(self.node)
    }

    pub fn child(&self, i: AccessChainIndex) -> Option<Self> {
        self.get()
            .children
            .get(&i)
            .map(|&n| Self::new(self.tree, n))
    }

    pub fn has_any_child(&self) -> bool {
        !self.get().children.is_empty()
    }

    pub fn has_child(&self, i: AccessChainIndex) -> bool {
        let children = &self.get().children;
        children.is_empty() || children.contains_key(&i)
    }

    pub fn children(&'a self) -> impl Iterator<Item = (AccessChainIndex, Self)> {
        self.get()
            .children
            .iter()
            .map(move |(&i, &n)| (i, Self::new(self.tree, n)))
    }
}

macro_rules! safe {
    ($($x:tt)*) => {
        unsafe {
            $($x)*
        }
    };
}

impl AccessTree {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            _storage: Vec::new(),
        }
    }

    pub fn get(&self, node: AccessNodeRef) -> &AccessNode {
        &self._storage[node.0]
    }

    pub fn get_mut(&mut self, node: AccessNodeRef) -> &mut AccessNode {
        &mut self._storage[node.0]
    }

    pub fn add_node(&mut self) -> AccessNodeRef {
        let index = self._storage.len();
        self._storage.push(AccessNode {
            children: HashMap::new(),
        });
        AccessNodeRef(index)
    }

    pub fn push(&mut self, node: AccessNode) -> AccessNodeRef {
        let index = self._storage.len();
        self._storage.push(node);
        AccessNodeRef(index)
    }

    // insert a new node into the tree
    pub fn insert(&mut self, node: NodeRef, access_chain: &[AccessChainIndex]) {
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

    pub fn insert_unrolled(&mut self, node: NodeRef, const_eval: &mut ConstEval) {
        if node.is_local() || node.is_gep() {
            let (root, chain) = Self::access_chain_from_gep_chain(node);
            let chain = Self::partially_evaluate_access_chain(&chain, const_eval);
            self.insert(root, &chain);
        } else {
            self.insert(node, &[]);
        }
    }

    // check if a node is accessed with the given access chain
    pub fn contains(&self, node: NodeRef, access_chain: &[AccessChainIndex]) -> bool {
        if let Some(access_node_ref) = self.nodes.get(&node).cloned() {
            let mut access_node_ref = access_node_ref;
            for i in access_chain {
                if self.get(access_node_ref).children.is_empty() {
                    // The node is accessed as a whole, so any further access to its children is contained
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

    // check if a node overlaps with the given access chain
    pub fn maybe_overlaps(&self, node: NodeRef, access_chain: &[AccessChainIndex]) -> bool {
        if let Some(access_node_ref) = self.nodes.get(&node).cloned() {
            let mut access_node_ref = access_node_ref;
            for i in access_chain {
                if self.get(access_node_ref).children.is_empty() {
                    // The node is accessed as a whole, so any further access to its children is contained
                    return true;
                } else if let Some(child) = self.get(access_node_ref).children.get(i).cloned() {
                    // go into the child node if it exists
                    access_node_ref = child;
                } else if matches!(i, AccessChainIndex::Dynamic(_))
                    || self
                        .get(access_node_ref)
                        .children
                        .iter()
                        .any(|(&i, _)| matches!(i, AccessChainIndex::Dynamic(_)))
                {
                    // the child node does not exist, but the access chains are dynamic, so we cannot
                    // determine if the chains overlap, so we conservatively assume that they do
                    return true;
                } else {
                    // the child node does not exist, so the chains are static and do not overlap
                    return false;
                }
            }
            // the access chain is a prefix of the node's access chain, so they overlap
            true
        } else {
            false
        }
    }

    pub fn clone_node(&mut self, a: AccessTreeNodeRef) -> AccessNodeRef {
        let new_node = AccessNode {
            children: a
                .children()
                .map(|(i, node)| (i, self.clone_node(node)))
                .collect(),
        };
        self.push(new_node)
    }

    pub fn intersect_nodes(
        &mut self,
        a: AccessTreeNodeRef,
        b: AccessTreeNodeRef,
    ) -> Option<AccessNodeRef> {
        if !a.has_any_child() {
            // a is accessed as a whole (a is a full set), then the intersection is b
            Some(self.clone_node(b))
        } else if !b.has_any_child() {
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
    pub fn intersect(&self, other: &Self) -> Self {
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

    pub fn union_nodes(&mut self, a: AccessTreeNodeRef, b: AccessTreeNodeRef) -> AccessNodeRef {
        if !a.has_any_child() {
            // a is accessed as a whole (a is a full set), then the union is a
            self.clone_node(a)
        } else if !b.has_any_child() {
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

    pub fn union(&self, other: &Self) -> Self {
        let mut new_tree = Self::new();
        for common_node in self.nodes.keys().filter(|k| other.nodes.contains_key(k)) {
            let merged = new_tree.union_nodes(
                AccessTreeNodeRef::new(self, self.nodes[common_node]),
                AccessTreeNodeRef::new(other, other.nodes[common_node]),
            );
            new_tree.nodes.insert(common_node.clone(), merged);
        }
        for node in self.nodes.keys().filter(|k| !other.nodes.contains_key(k)) {
            let merged = new_tree.clone_node(AccessTreeNodeRef::new(self, self.nodes[node]));
            new_tree.nodes.insert(node.clone(), merged);
        }
        for node in other.nodes.keys().filter(|k| !self.nodes.contains_key(k)) {
            let merged = new_tree.clone_node(AccessTreeNodeRef::new(other, other.nodes[node]));
            new_tree.nodes.insert(node.clone(), merged);
        }
        new_tree
    }

    fn _enumerate_chains_for_subtraction(
        &self,
        node: NodeRef,
        access_node: &AccessNode,
        access_chain: &mut Vec<AccessChainIndex>,
        other: &Self,
        result: &mut Self,
    ) {
        if access_node.children.is_empty() {
            // leaf node
            if !other.contains(node, access_chain) {
                result.insert(node, access_chain);
            }
        } else {
            // non-leaf node
            for (i, access_node) in access_node.children.iter() {
                match i {
                    AccessChainIndex::Static(_) => {
                        access_chain.push(*i);
                        self._enumerate_chains_for_subtraction(
                            node,
                            self.get(*access_node),
                            access_chain,
                            other,
                            result,
                        );
                        access_chain.pop();
                    }
                    AccessChainIndex::Dynamic(_) => unimplemented!("dynamic access chains"),
                }
            }
        }
    }

    // subtract two access trees (a - b)
    pub fn subtract(&self, other: &Self) -> Self {
        // enumerate all paths in the tree and insert them if not contained in other
        let mut new_tree = Self::new();
        let mut access_chain = Vec::new();
        for (node, &node_ref) in self.nodes.iter() {
            access_chain.clear();
            self._enumerate_chains_for_subtraction(
                *node,
                self.get(node_ref),
                &mut access_chain,
                other,
                &mut new_tree,
            );
        }
        new_tree
    }

    // if all children of a node are accessed as a whole, then we can coalesce them into
    // a single leaf node (i.e., removing all of its children)
    pub fn coalesce_whole_access_chains(&mut self) {
        let this = safe! { &mut *(self as *mut Self) };
        for (&node, &node_ref) in self.nodes.iter() {
            this._coalesce_whole_access_chains(node_ref, node.type_().as_ref());
        }
    }

    fn _coalesce_whole_access_chains(&mut self, node_ref: AccessNodeRef, t: &Type) {
        let this = safe! { &mut *(self as *mut Self) };
        let full_children_count = self
            .get(node_ref)
            .children
            .iter()
            .filter(|(&i, &n)| {
                if let AccessChainIndex::Static(i) = i {
                    this._coalesce_whole_access_chains(n, t.extract(i as usize).as_ref());
                    this.get(n).children.is_empty()
                } else {
                    false
                }
            })
            .count();
        let dim = match t {
            Type::Vector(v) => v.length as usize,
            Type::Matrix(m) => m.dimension as usize,
            Type::Array(a) => a.length,
            Type::Struct(s) => s.fields.len(),
            _ => 0,
        };
        if full_children_count == dim {
            self.get_mut(node_ref).children.clear();
        }
    }

    // if a node is accessed with dynamic indices at some level of the access chain, then
    // we conservatively assume that all of its children are accessed with dynamic indices
    // and collapse the access chain into a single node (i.e., removing all of its children)
    pub fn dynamic_access_chains_as_whole(&mut self) {
        let this = safe! { &mut *(self as *mut Self) };
        for (&node, &node_ref) in self.nodes.iter() {
            this._collapse_dynamic_access_chains(node_ref, node.type_().as_ref());
        }
    }

    fn _collapse_dynamic_access_chains(&mut self, node_ref: AccessNodeRef, t: &Type) {
        let has_dynamic_access = self.get(node_ref).children.iter().any(|(&i, _)| match i {
            AccessChainIndex::Dynamic(_) => true,
            _ => false,
        });
        if has_dynamic_access {
            self.get_mut(node_ref).children.clear();
        } else {
            let this = safe! { &mut *(self as *mut Self) };
            for (&i, &child) in self.get(node_ref).children.iter() {
                if let AccessChainIndex::Static(index) = i {
                    let elem = t.extract(index as usize);
                    this._collapse_dynamic_access_chains(child, &elem);
                } else {
                    unreachable!()
                }
            }
        }
    }

    pub fn partially_evaluate_access_chain(
        indices: &[NodeRef],
        const_eval: &mut ConstEval,
    ) -> Vec<AccessChainIndex> {
        indices
            .iter()
            .map(|&i| {
                if let Some(value) = const_eval.eval(i).and_then(|v| v.try_get_i32()) {
                    value.into()
                } else {
                    i.into()
                }
            })
            .collect()
    }

    pub fn access_chain_from_gep_chain(node: NodeRef) -> (NodeRef, Vec<NodeRef>) {
        if let Instruction::Call(Func::GetElementPtr, args) = node.get().instruction.as_ref() {
            assert!(!args[0].is_gep(), "nested GEP is not supported");
            (args[0], args.iter().skip(1).cloned().collect())
        } else {
            (node, Vec::new())
        }
    }
}

impl AccessTree {
    fn packed_aggregate_size(t: &Type) -> usize {
        match t {
            Type::Vector(v) => v.element().size() * v.length as usize,
            Type::Matrix(m) => m.element().size() * m.dimension as usize * m.dimension as usize,
            Type::Struct(s) => s
                .fields
                .iter()
                .map(|f| Self::packed_aggregate_size(f.as_ref()))
                .sum(),
            _ => t.size(),
        }
    }
    fn compute_memory_footprint(uses: &AccessTree, node: AccessNodeRef, t: &Type) -> usize {
        let node = uses.get(node);
        if node.children.is_empty() {
            Self::packed_aggregate_size(t)
        } else {
            node.children
                .iter()
                .map(|(&i, &child)| match i {
                    AccessChainIndex::Static(i) => {
                        Self::compute_memory_footprint(uses, child, t.extract(i as usize).as_ref())
                    }
                    _ => unreachable!("access chain should be truncated at dynamic indices"),
                })
                .sum()
        }
    }
    pub fn dump(&self) {
        let mut total_size = 0usize;
        for (i, (node, access_node)) in self.nodes.iter().enumerate() {
            let node = node.get();
            let t = node.type_.clone();
            let size = Self::compute_memory_footprint(self, *access_node, &t);
            println!("  #{:<3} {:?} (size = {})", i, node.type_.as_ref(), size);
            println!("       {:?}", node.instruction.as_ref());
            total_size += size;
        }
        println!("  Total Size = {}", total_size);
    }
}
