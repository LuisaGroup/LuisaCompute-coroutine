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
use crate::ir::{BasicBlock, CallableModule, Func, Instruction, NodeRef, Usage};
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

    fn children(&'a self) -> impl Iterator<Item = (AccessChainIndex, Self)> {
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

    fn analyze_branch_block_in_simple(
        &self,
        block: &BasicBlock,
        parent_defs: &AccessTree,
        def_use: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) -> AccessTree {
        let mut defs = parent_defs.clone();
        self.analyze_direct_block_in_simple(block, &mut defs, def_use, helpers);
        defs
    }

    fn analyze_direct_block_in_simple(
        &self,
        block: &BasicBlock,
        defs: &mut AccessTree,
        def_use: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        for node in block.iter() {
            self.analyze_simple(node, defs, def_use, helpers);
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

    fn _access_chain_from_gep_chain(
        gep_or_local: NodeRef,
        chain: &mut Vec<NodeRef>,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) -> NodeRef /* root */ {
        if gep_or_local.is_local() {
            gep_or_local
        } else if let Instruction::Call(Func::GetElementPtr, args) =
            gep_or_local.get().instruction.as_ref()
        {
            let root = Self::_access_chain_from_gep_chain(args[0], chain, helpers);
            chain.extend(args.iter().skip(1));
            root
        } else {
            unreachable!()
        }
    }

    fn access_chain_from_gep_chain(
        gep_or_local: NodeRef,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) -> (NodeRef, Vec<NodeRef>) {
        let mut chain = Vec::new();
        let root = Self::_access_chain_from_gep_chain(gep_or_local, &mut chain, helpers);
        (root, chain)
    }

    fn analyze_load_chain(
        &self,
        loaded: NodeRef,
        defs: &mut AccessTree,
        result: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        let (root, chain) = Self::access_chain_from_gep_chain(loaded, helpers);
        self.mark_use(root, &chain, defs, result, helpers);
    }

    fn analyze_update_chain(
        &self,
        updated: NodeRef,
        defs: &mut AccessTree,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        let (root, chain) = Self::access_chain_from_gep_chain(updated, helpers);
        self.mark_def(root, &chain, defs, helpers);
    }

    fn analyze_callable(
        &self,
        ret: NodeRef,
        callable: &CallableModule,
        args: &[NodeRef],
        defs: &mut AccessTree,
        result: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        for (i, arg) in args.iter().enumerate() {
            match helpers.callable_args.get_possible_usage(callable, i, &[]) {
                Usage::READ | Usage::READ_WRITE => {
                    self.mark_use_with_possible_implicit_load(*arg, defs, result, helpers)
                }
                _ => {}
            }
        }
        if !callable.ret_type.is_void() {
            self.mark_def(ret, &[], defs, helpers);
        }
    }

    fn analyze_call(
        &self,
        ret: NodeRef,
        func: &Func,
        args: &[NodeRef],
        defs: &mut AccessTree,
        result: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        match func {
            Func::ZeroInitializer
            | Func::ThreadId
            | Func::BlockId
            | Func::WarpSize
            | Func::WarpLaneId
            | Func::DispatchId
            | Func::DispatchSize
            | Func::CoroId
            | Func::CoroToken => {
                self.mark_def(ret, &[], defs, helpers);
            }
            Func::Assume | Func::Assert(_) => {
                self.mark_use_with_possible_implicit_load(args[0], defs, result, helpers);
            }
            Func::Unreachable(_) => {
                if !ret.type_().is_void() {
                    self.mark_def(ret, &[], defs, helpers);
                }
            }
            Func::PropagateGrad => todo!(),
            Func::OutputGrad => todo!(),
            Func::RequiresGradient => todo!(),
            Func::Backward => todo!(),
            Func::Gradient => todo!(),
            Func::GradientMarker => todo!(),
            Func::AccGrad => todo!(),
            Func::Detach => todo!(),
            Func::RayTracingInstanceTransform
            | Func::RayTracingInstanceVisibilityMask
            | Func::RayTracingInstanceUserId
            | Func::RayTracingTraceClosest
            | Func::RayTracingTraceAny
            | Func::RayTracingQueryAll
            | Func::RayTracingQueryAny
            | Func::RayQueryWorldSpaceRay
            | Func::RayQueryProceduralCandidateHit
            | Func::RayQueryTriangleCandidateHit
            | Func::RayQueryCommittedHit => {
                for &arg in args {
                    self.mark_use_with_possible_implicit_load(arg, defs, result, helpers);
                }
                self.mark_def(ret, &[], defs, helpers);
            }
            Func::RayTracingSetInstanceTransform
            | Func::RayTracingSetInstanceOpacity
            | Func::RayTracingSetInstanceVisibility
            | Func::RayTracingSetInstanceUserId
            | Func::RayQueryCommitTriangle
            | Func::RayQueryCommitProcedural
            | Func::RayQueryTerminate => {
                for &arg in args {
                    self.mark_use_with_possible_implicit_load(arg, defs, result, helpers);
                }
            }
            Func::RasterDiscard | Func::SynchronizeBlock => {}
            Func::IndirectDispatchSetCount | Func::IndirectDispatchSetKernel => {
                for &arg in args {
                    self.mark_use_with_possible_implicit_load(arg, defs, result, helpers);
                }
            }
            Func::Load => {
                self.analyze_load_chain(args[0], defs, result, helpers);
                self.mark_def(ret, &[], defs, helpers);
            }
            Func::GetElementPtr => {
                for &arg in args.iter().skip(1) {
                    self.mark_use_with_possible_implicit_load(arg, defs, result, helpers);
                }
                // the parent node args[0] will be analyzed at the use site, e.g., when updated/loaded
            }
            // (args...) -> ret, simply use the args and define the ret
            Func::AddressOf
            | Func::Cast
            | Func::Bitcast
            | Func::Pack
            | Func::Unpack
            | Func::Add
            | Func::Sub
            | Func::Mul
            | Func::Div
            | Func::Rem
            | Func::BitAnd
            | Func::BitOr
            | Func::BitXor
            | Func::Shl
            | Func::Shr
            | Func::RotRight
            | Func::RotLeft
            | Func::Eq
            | Func::Ne
            | Func::Lt
            | Func::Le
            | Func::Gt
            | Func::Ge
            | Func::MatCompMul
            | Func::Neg
            | Func::Not
            | Func::BitNot
            | Func::All
            | Func::Any
            | Func::Select
            | Func::Clamp
            | Func::Lerp
            | Func::Step
            | Func::SmoothStep
            | Func::Saturate
            | Func::Abs
            | Func::Min
            | Func::Max
            | Func::ReduceSum
            | Func::ReduceProd
            | Func::ReduceMin
            | Func::ReduceMax
            | Func::Clz
            | Func::Ctz
            | Func::PopCount
            | Func::Reverse
            | Func::IsInf
            | Func::IsNan
            | Func::Acos
            | Func::Acosh
            | Func::Asin
            | Func::Asinh
            | Func::Atan
            | Func::Atan2
            | Func::Atanh
            | Func::Cos
            | Func::Cosh
            | Func::Sin
            | Func::Sinh
            | Func::Tan
            | Func::Tanh
            | Func::Exp
            | Func::Exp2
            | Func::Exp10
            | Func::Log
            | Func::Log2
            | Func::Log10
            | Func::Powi
            | Func::Powf
            | Func::Sqrt
            | Func::Rsqrt
            | Func::Ceil
            | Func::Floor
            | Func::Fract
            | Func::Trunc
            | Func::Round
            | Func::Fma
            | Func::Copysign
            | Func::Cross
            | Func::Dot
            | Func::OuterProduct
            | Func::Length
            | Func::LengthSquared
            | Func::Normalize
            | Func::Faceforward
            | Func::Distance
            | Func::Reflect
            | Func::Determinant
            | Func::Transpose
            | Func::Inverse
            | Func::Vec
            | Func::Vec2
            | Func::Vec3
            | Func::Vec4
            | Func::Permute
            | Func::InsertElement
            | Func::ExtractElement
            | Func::Struct
            | Func::Array
            | Func::Mat
            | Func::Mat2
            | Func::Mat3
            | Func::Mat4
            | Func::WarpIsFirstActiveLane
            | Func::WarpFirstActiveLane
            | Func::WarpActiveAllEqual
            | Func::WarpActiveBitAnd
            | Func::WarpActiveBitOr
            | Func::WarpActiveBitXor
            | Func::WarpActiveCountBits
            | Func::WarpActiveMax
            | Func::WarpActiveMin
            | Func::WarpActiveProduct
            | Func::WarpActiveSum
            | Func::WarpActiveAll
            | Func::WarpActiveAny
            | Func::WarpActiveBitMask
            | Func::WarpPrefixCountBits
            | Func::WarpPrefixSum
            | Func::WarpPrefixProduct
            | Func::WarpReadLaneAt
            | Func::WarpReadFirstLane
            | Func::AtomicExchange
            | Func::AtomicCompareExchange
            | Func::AtomicFetchAdd
            | Func::AtomicFetchSub
            | Func::AtomicFetchAnd
            | Func::AtomicFetchOr
            | Func::AtomicFetchXor
            | Func::AtomicFetchMin
            | Func::AtomicFetchMax
            | Func::BufferRead
            | Func::BufferSize
            | Func::BufferAddress
            | Func::ByteBufferRead
            | Func::ByteBufferSize
            | Func::Texture2dRead
            | Func::Texture2dSize
            | Func::Texture3dRead
            | Func::Texture3dSize
            | Func::BindlessTexture2dSample
            | Func::BindlessTexture2dSampleLevel
            | Func::BindlessTexture2dSampleGrad
            | Func::BindlessTexture2dSampleGradLevel
            | Func::BindlessTexture3dSample
            | Func::BindlessTexture3dSampleLevel
            | Func::BindlessTexture3dSampleGrad
            | Func::BindlessTexture3dSampleGradLevel
            | Func::BindlessTexture2dRead
            | Func::BindlessTexture3dRead
            | Func::BindlessTexture2dReadLevel
            | Func::BindlessTexture3dReadLevel
            | Func::BindlessTexture2dSize
            | Func::BindlessTexture3dSize
            | Func::BindlessTexture2dSizeLevel
            | Func::BindlessTexture3dSizeLevel
            | Func::BindlessBufferRead
            | Func::BindlessBufferSize
            | Func::BindlessBufferAddress
            | Func::BindlessBufferType
            | Func::BindlessByteBufferRead => {
                for &arg in args {
                    self.mark_use_with_possible_implicit_load(arg, defs, result, helpers);
                }
                self.mark_def(ret, &[], defs, helpers);
            }
            Func::AtomicRef => unreachable!("atomic ref should be lowered"),
            // void return, defines nothing
            Func::BufferWrite
            | Func::ByteBufferWrite
            | Func::Texture2dWrite
            | Func::Texture3dWrite
            | Func::BindlessBufferWrite
            | Func::ShaderExecutionReorder => {
                for &arg in args {
                    self.mark_use_with_possible_implicit_load(arg, defs, result, helpers);
                }
            }
            Func::CpuCustomOp(_) => todo!(),
            Func::Unknown0 => todo!(),
            Func::Unknown1 => todo!(),
            Func::Callable(callable) => {
                self.analyze_callable(ret, callable.0.as_ref(), args, defs, result, helpers);
            }
        }
    }

    fn mark_use_with_possible_implicit_load(
        &self,
        node: NodeRef,
        defs: &mut AccessTree,
        result: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        if node.is_local() || node.is_gep() {
            // implicit load
            self.analyze_load_chain(node, defs, result, helpers);
        } else {
            self.mark_use(node, &[], defs, result, helpers);
        }
    }

    fn analyze_simple(
        &self,
        node: NodeRef,
        defs: &mut AccessTree,
        result: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        match node.get().instruction.as_ref() {
            Instruction::Local { init } => {
                self.mark_use(*init, &[], defs, result, helpers);
                self.mark_def(node, &[], defs, helpers);
            }
            Instruction::Update { var, value } => {
                self.mark_use(*value, &[], defs, result, helpers);
                self.analyze_update_chain(*var, defs, helpers);
            }
            Instruction::Call(func, args) => {
                self.analyze_call(node, func, args.as_ref(), defs, result, helpers);
            }
            Instruction::Phi(_) => unreachable!("phis should be lowered"),
            Instruction::AdScope { .. } | Instruction::AdDetach(_) => todo!(),
            Instruction::Return(_) | Instruction::Break | Instruction::Continue => {
                unreachable!("returns, breaks and continues should be lowered")
            }
            // can still appear in RayQuery branches
            Instruction::Loop { body, cond } => {
                // do-while loop, so the condition is evaluated after the body
                self.analyze_direct_block_in_simple(body, defs, result, helpers);
                self.mark_use_with_possible_implicit_load(*cond, defs, result, helpers);
            }
            Instruction::GenericLoop { .. } => unreachable!("generic loops should be lowered"),
            Instruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                self.mark_use_with_possible_implicit_load(*cond, defs, result, helpers);
                let true_defs =
                    self.analyze_branch_block_in_simple(true_branch, defs, result, helpers);
                let false_defs =
                    self.analyze_branch_block_in_simple(false_branch, defs, result, helpers);
                *defs = AccessTree::intersect(&true_defs, &false_defs);
            }
            Instruction::Switch {
                value,
                cases,
                default,
            } => {
                self.mark_use_with_possible_implicit_load(*value, defs, result, helpers);
                let mut common_defs =
                    self.analyze_branch_block_in_simple(default, defs, result, helpers);
                for case in cases.iter() {
                    let case_defs =
                        self.analyze_branch_block_in_simple(&case.block, defs, result, helpers);
                    common_defs = AccessTree::intersect(&common_defs, &case_defs);
                }
                *defs = common_defs;
            }
            Instruction::RayQuery {
                ray_query,
                on_triangle_hit,
                on_procedural_hit,
            } => {
                // TODO: this is actually not necessary as we do not allow ray queries to be
                //   split into multiple coroutine scopes
                self.mark_use_with_possible_implicit_load(*ray_query, defs, result, helpers);
                self.analyze_branch_block_in_simple(on_triangle_hit, defs, result, helpers);
                self.analyze_branch_block_in_simple(on_procedural_hit, defs, result, helpers);
                // note that we do not propagate defs from ray query branches since
                // both branches might not be executed
            }
            Instruction::Print { fmt, args } => {
                for arg in args.iter() {
                    self.mark_use_with_possible_implicit_load(*arg, defs, result, helpers);
                }
            }
            Instruction::CoroRegister { value, .. } => {
                self.mark_use_with_possible_implicit_load(*value, defs, result, helpers);
            }
            _ => {}
        }
    }

    fn analyze_instr(
        &self,
        instr: &CoroInstruction,
        defs: &mut AccessTree,
        result: &mut CoroScopeDefUse,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        match instr {
            CoroInstruction::Entry | CoroInstruction::EntryScope { .. } => {
                unreachable!("entry scopes are only allowed in preliminary coroutine graphs")
            }
            CoroInstruction::Simple(node) => {
                self.analyze_simple(*node, defs, result, helpers);
            }
            CoroInstruction::ConditionStackReplay { items } => {
                for item in items {
                    if item.node.is_local() {
                        self.analyze_update_chain(item.node, defs, helpers);
                    } else {
                        self.mark_def(item.node, &[], defs, helpers);
                    }
                }
            }
            CoroInstruction::MakeFirstFlag | CoroInstruction::ClearFirstFlag(_) => {
                // nothing to do as the first flag is always defined locally
            }
            CoroInstruction::SkipIfFirstFlag { body, .. } => {
                // note that we can not propagate defs from the body since the body
                // might not be executed
                self.analyze_branch_block(body, defs, result, helpers);
            }
            CoroInstruction::Loop { body, cond } => {
                self.analyze_direct_block(body, defs, result, helpers);
                if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                    self.mark_use_with_possible_implicit_load(cond.clone(), defs, result, helpers);
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
                    self.mark_use_with_possible_implicit_load(cond.clone(), defs, result, helpers);
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
                    self.mark_use_with_possible_implicit_load(cond.clone(), defs, result, helpers);
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
