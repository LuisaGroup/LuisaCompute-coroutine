// This file implement use-def analysis for coroutine scopes.
// For each coroutine scope, at each instruction, the analysis will record the set of
// definitions that are alive at that instruction. If the instruction uses some value
// that is not alive at that instruction, then the value must be defined other places
// so we would have to pass the value through the coroutine frame.
// The analysis tries to perform member-wise analysis, so the values that have to be
// passed through the coroutine frame are minimized.
// Specifically, the analysis computes the following sets for each coroutine scope:
// - ExternalUse: nodes used inside the scope but defined outside the scope
// - InternalTouch: nodes touched (possibly overwritten) in the scope
// - InternalKill: nodes killed (definitely overwritten) at each suspend point

// TODO: compute the touch set per suspend point

use crate::analysis::callable_arg_usages::CallableArgumentUsageAnalysis;
use crate::analysis::const_eval::ConstEval;
use crate::analysis::coro_graph::{
    CoroGraph, CoroInstrRef, CoroInstruction, CoroScope, CoroScopeRef,
};
use crate::analysis::replayable_values::ReplayableValueAnalysis;
use crate::analysis::utility::AccessTree;
use crate::ir::{BasicBlock, CallableModule, Func, Instruction, NodeRef, Usage};
use std::collections::HashMap;

// In the structured IR, an instruction dominates all successive instructions in the
// same basic block. So the use-def analysis can be performed in a single pass:
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

pub(crate) struct CoroScopeUseDef {
    // uses that are not dominated by defs in the current scope
    pub external_uses: AccessTree,
    // access chains that might be written to in the current scope
    pub internal_touches: AccessTree,
    // defs at the suspend point that can be safely localized in the current scope as
    // they kill the possible definitions that are passed in through the frame
    // note: the scope might define other values at the suspend point, but they are
    //       unsafe to localize because their defs do not dominate the suspend point
    pub internal_kills: HashMap<CoroScopeRef, AccessTree>,
}

impl CoroScopeUseDef {
    pub fn new() -> Self {
        Self {
            external_uses: AccessTree::new(),
            internal_touches: AccessTree::new(),
            internal_kills: HashMap::new(),
        }
    }
}

pub(crate) struct CoroUseDefAnalysis<'a> {
    graph: &'a CoroGraph,
}

struct CoroDefUseHelperAnalyses {
    replayable: ReplayableValueAnalysis,
    callable_args: CallableArgumentUsageAnalysis,
    const_eval: ConstEval,
}

impl<'a> CoroUseDefAnalysis<'a> {
    fn analyze_branch_block(
        &self,
        block: &Vec<CoroInstrRef>,
        parent_kills: &AccessTree,
        use_def: &mut CoroScopeUseDef,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) -> AccessTree {
        let mut kills = parent_kills.clone();
        self.analyze_direct_block(block, &mut kills, use_def, helpers);
        kills
    }

    fn analyze_direct_block(
        &self,
        block: &Vec<CoroInstrRef>,
        kills: &mut AccessTree,
        use_def: &mut CoroScopeUseDef,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        for &instr in block.iter() {
            self.analyze_instr(self.graph.get_instr(instr), kills, use_def, helpers);
        }
    }

    fn analyze_branch_block_in_simple(
        &self,
        block: &BasicBlock,
        parent_kills: &AccessTree,
        use_def: &mut CoroScopeUseDef,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) -> AccessTree {
        let mut kills = parent_kills.clone();
        self.analyze_direct_block_in_simple(block, &mut kills, use_def, helpers);
        kills
    }

    fn analyze_direct_block_in_simple(
        &self,
        block: &BasicBlock,
        kills: &mut AccessTree,
        use_def: &mut CoroScopeUseDef,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        for node in block.iter() {
            self.analyze_simple(node, kills, use_def, helpers);
        }
    }

    fn mark_use(
        &self,
        node_ref: NodeRef,
        kills: &mut AccessTree,
        result: &mut CoroScopeUseDef,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        if helpers.replayable.detect(node_ref) {
            // the value is replayable, so we don't need to pass it through the frame
            return;
        }
        // otherwise, check if the value is dominated by defs in the current scope
        let (root, access_chain) = AccessTree::access_chain_from_gep_chain(node_ref);
        if root.is_reference_argument() {
            return;
        }
        let access_chain =
            AccessTree::partially_evaluate_access_chain(&access_chain, &mut helpers.const_eval);
        if !kills.contains(root, access_chain.as_slice()) {
            // try coalescing the def tree to reduce the number of external uses
            kills.coalesce_whole_access_chains();
            if !kills.contains(root, access_chain.as_slice()) {
                // the value is not dominated by defs in the current scope, so we need to
                // pass it through the frame
                result.external_uses.insert(root, access_chain.as_slice());
            }
        }
    }

    fn mark_touch(
        &self,
        node_ref: NodeRef,
        result: &mut CoroScopeUseDef,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        let (root, access_chain) = AccessTree::access_chain_from_gep_chain(node_ref);
        if root.is_reference_argument() {
            return;
        }
        let access_chain =
            AccessTree::partially_evaluate_access_chain(&access_chain, &mut helpers.const_eval);
        result
            .internal_touches
            .insert(root, access_chain.as_slice());
    }

    fn mark_kill(
        &self,
        node_ref: NodeRef,
        kills: &mut AccessTree,
        result: &mut CoroScopeUseDef,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        let (root, access_chain) = AccessTree::access_chain_from_gep_chain(node_ref);
        let access_chain =
            AccessTree::partially_evaluate_access_chain(&access_chain, &mut helpers.const_eval);
        kills.insert(root, access_chain.as_slice());
        // a kill always touches the value
        result
            .internal_touches
            .insert(root, access_chain.as_slice());
    }

    fn analyze_callable(
        &self,
        ret: NodeRef,
        callable: &CallableModule,
        args: &[NodeRef],
        kills: &mut AccessTree,
        result: &mut CoroScopeUseDef,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        for (i, arg) in args.iter().enumerate() {
            match helpers.callable_args.get_possible_usage(callable, i, &[]) {
                Usage::READ => self.mark_use(*arg, kills, result, helpers),
                Usage::WRITE => {
                    self.mark_touch(*arg, result, helpers);
                }
                Usage::READ_WRITE => {
                    self.mark_use(*arg, kills, result, helpers);
                    self.mark_touch(*arg, result, helpers);
                }
                Usage::NONE => {}
            }
        }
        if !callable.ret_type.is_void() {
            self.mark_kill(ret, kills, result, helpers);
        }
    }

    fn analyze_call(
        &self,
        ret: NodeRef,
        func: &Func,
        args: &[NodeRef],
        kills: &mut AccessTree,
        result: &mut CoroScopeUseDef,
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
                self.mark_kill(ret, kills, result, helpers);
            }
            Func::Assume | Func::Assert(_) => {
                self.mark_use(args[0], kills, result, helpers);
            }
            Func::Unreachable(_) => {
                if !ret.type_().is_void() {
                    self.mark_kill(ret, kills, result, helpers);
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
                    self.mark_use(arg, kills, result, helpers);
                }
                self.mark_kill(ret, kills, result, helpers);
            }
            Func::RayTracingSetInstanceTransform
            | Func::RayTracingSetInstanceOpacity
            | Func::RayTracingSetInstanceVisibility
            | Func::RayTracingSetInstanceUserId => {
                for &arg in args {
                    self.mark_use(arg, kills, result, helpers);
                }
            }
            Func::RayQueryCommitTriangle
            | Func::RayQueryCommitProcedural
            | Func::RayQueryTerminate => {
                for &arg in args {
                    self.mark_use(arg, kills, result, helpers);
                }
                self.mark_touch(args[0], result, helpers);
            }
            Func::RasterDiscard | Func::SynchronizeBlock => {}
            Func::IndirectDispatchSetCount | Func::IndirectDispatchSetKernel => {
                for &arg in args {
                    self.mark_use(arg, kills, result, helpers);
                }
            }
            Func::Load => {
                self.mark_use(args[0], kills, result, helpers);
                self.mark_kill(ret, kills, result, helpers);
            }
            Func::GetElementPtr => {
                assert!(!args[0].is_gep(), "Nested GEP's should have been lowered!");
                // mark that the indices are used
                for &arg in args.iter().skip(1) {
                    self.mark_use(arg, kills, result, helpers);
                }
                // the parent node args[0] will be analyzed at the use site, e.g., when updated/loaded
            }
            Func::AddressOf => {
                // sadly we have to conservatively assume that anything can happen to the value that
                // we take the address of, so we mark it as used and touched
                self.mark_touch(args[0], result, helpers);
                self.mark_use(args[0], kills, result, helpers);
                // the result is always a kill
                self.mark_kill(ret, kills, result, helpers);
            }
            // (args...) -> ret, simply use the args and define the ret
            Func::Cast
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
                    self.mark_use(arg, kills, result, helpers);
                }
                self.mark_kill(ret, kills, result, helpers);
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
                    self.mark_use(arg, kills, result, helpers);
                }
            }
            Func::CpuCustomOp(_) => todo!(),
            Func::Unknown0 => todo!(),
            Func::Unknown1 => todo!(),
            Func::Callable(callable) => {
                self.analyze_callable(ret, callable.0.as_ref(), args, kills, result, helpers);
            }
        }
    }

    fn analyze_simple(
        &self,
        node: NodeRef,
        kills: &mut AccessTree,
        result: &mut CoroScopeUseDef,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        match node.get().instruction.as_ref() {
            Instruction::Local { init } => {
                self.mark_use(*init, kills, result, helpers);
                self.mark_kill(node, kills, result, helpers);
            }
            Instruction::Update { var, value } => {
                self.mark_use(*value, kills, result, helpers);
                self.mark_kill(*var, kills, result, helpers);
            }
            Instruction::Call(func, args) => {
                self.analyze_call(node, func, args.as_ref(), kills, result, helpers);
            }
            Instruction::Phi(_) => unreachable!("phis should be lowered"),
            Instruction::AdScope { .. } | Instruction::AdDetach(_) => todo!(),
            Instruction::Return(_) | Instruction::Break | Instruction::Continue => {
                unreachable!("returns, breaks and continues should be lowered")
            }
            // can still appear in RayQuery branches
            Instruction::Loop { body, cond } => {
                // do-while loop, so the condition is evaluated after the body
                self.analyze_direct_block_in_simple(body, kills, result, helpers);
                self.mark_use(*cond, kills, result, helpers);
            }
            Instruction::GenericLoop { .. } => unreachable!("generic loops should be lowered"),
            Instruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                self.mark_use(*cond, kills, result, helpers);
                let true_defs =
                    self.analyze_branch_block_in_simple(true_branch, kills, result, helpers);
                let false_defs =
                    self.analyze_branch_block_in_simple(false_branch, kills, result, helpers);
                *kills = AccessTree::intersect(&true_defs, &false_defs);
            }
            Instruction::Switch {
                value,
                cases,
                default,
            } => {
                self.mark_use(*value, kills, result, helpers);
                let mut common_defs =
                    self.analyze_branch_block_in_simple(default, kills, result, helpers);
                for case in cases.iter() {
                    let case_defs =
                        self.analyze_branch_block_in_simple(&case.block, kills, result, helpers);
                    common_defs = AccessTree::intersect(&common_defs, &case_defs);
                }
                *kills = common_defs;
            }
            Instruction::RayQuery {
                ray_query,
                on_triangle_hit,
                on_procedural_hit,
            } => {
                // TODO: this is actually not necessary as we do not allow ray queries to be
                //   split into multiple coroutine scopes
                self.mark_use(*ray_query, kills, result, helpers);
                self.analyze_branch_block_in_simple(on_triangle_hit, kills, result, helpers);
                self.analyze_branch_block_in_simple(on_procedural_hit, kills, result, helpers);
                // note that we do not propagate defs from ray query branches since
                // both branches might not be executed
            }
            Instruction::Print { fmt, args } => {
                for arg in args.iter() {
                    self.mark_use(*arg, kills, result, helpers);
                }
            }
            Instruction::CoroRegister { value, .. } => {
                self.mark_use(*value, kills, result, helpers);
            }
            _ => {}
        }
    }

    fn analyze_instr(
        &self,
        instr: &CoroInstruction,
        kills: &mut AccessTree,
        result: &mut CoroScopeUseDef,
        helpers: &mut CoroDefUseHelperAnalyses,
    ) {
        match instr {
            CoroInstruction::Entry | CoroInstruction::EntryScope { .. } => {
                unreachable!("entry scopes are only allowed in preliminary coroutine graphs")
            }
            CoroInstruction::Simple(node) => {
                self.analyze_simple(*node, kills, result, helpers);
            }
            CoroInstruction::ConditionStackReplay { items } => {
                for item in items {
                    self.mark_kill(item.node, kills, result, helpers);
                }
            }
            CoroInstruction::MakeFirstFlag | CoroInstruction::ClearFirstFlag(_) => {
                // nothing to do as the first flag is always defined locally
            }
            CoroInstruction::SkipIfFirstFlag { body, .. } => {
                // note that we can not propagate defs from the body since the body
                // might not be executed
                self.analyze_branch_block(body, kills, result, helpers);
            }
            CoroInstruction::Loop { body, cond } => {
                self.analyze_direct_block(body, kills, result, helpers);
                if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                    self.mark_use(cond.clone(), kills, result, helpers);
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
                    self.mark_use(cond.clone(), kills, result, helpers);
                } else {
                    unreachable!()
                }
                let true_defs = self.analyze_branch_block(true_branch, kills, result, helpers);
                let false_defs = self.analyze_branch_block(false_branch, kills, result, helpers);
                *kills = AccessTree::intersect(&true_defs, &false_defs);
            }
            CoroInstruction::Switch {
                cond,
                cases,
                default,
            } => {
                if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                    self.mark_use(cond.clone(), kills, result, helpers);
                } else {
                    unreachable!()
                }
                let mut common_defs = self.analyze_branch_block(default, kills, result, helpers);
                for case in cases {
                    let case_defs = self.analyze_branch_block(&case.body, kills, result, helpers);
                    common_defs = AccessTree::intersect(&common_defs, &case_defs);
                }
                *kills = common_defs;
            }
            CoroInstruction::Suspend { token } => {
                // record the defs at the suspend point
                let scope_ref = self.graph.tokens[token];
                if let Some(d) = result.internal_kills.get_mut(&scope_ref) {
                    *d = AccessTree::intersect(d, kills);
                    d.coalesce_whole_access_chains();
                } else {
                    kills.coalesce_whole_access_chains();
                    result.internal_kills.insert(scope_ref, kills.clone());
                }
            }
            CoroInstruction::Terminate => {}
        }
    }
}

pub(crate) struct CoroGraphUseDef {
    pub scopes: HashMap<CoroScopeRef, CoroScopeUseDef>,
    pub union_uses: AccessTree,
}

impl CoroGraphUseDef {
    pub fn dump(&self) {
        println!("===================== CoroGraph Use-Def =====================");
        for i in 0..self.scopes.len() {
            let scope = &self.scopes[&CoroScopeRef(i)];
            println!(
                "=============== Scope #{} External Uses (N = {}) ===============",
                i,
                scope.external_uses.nodes.len()
            );
            scope.external_uses.dump();
        }
        println!(
            "================ Union of External Uses (N = {}) ================",
            self.union_uses.nodes.len()
        );
        self.union_uses.dump();
    }
}

impl<'a> CoroUseDefAnalysis<'a> {
    pub fn new(graph: &'a CoroGraph) -> Self {
        Self { graph }
    }

    pub fn analyze_scope(&self, scope: &CoroScope) -> CoroScopeUseDef {
        let mut use_def = CoroScopeUseDef::new();
        self.analyze_direct_block(
            &scope.instructions,
            &mut AccessTree::new(),
            &mut use_def,
            &mut CoroDefUseHelperAnalyses {
                replayable: ReplayableValueAnalysis::new(false),
                callable_args: CallableArgumentUsageAnalysis::new(),
                const_eval: ConstEval::new(),
            },
        );
        // postprocess the result
        use_def.internal_touches.coalesce_whole_access_chains();
        use_def.internal_touches.dynamic_access_chains_as_whole();
        use_def.external_uses.coalesce_whole_access_chains();
        use_def.external_uses.dynamic_access_chains_as_whole();
        use_def
    }

    pub fn analyze_graph(&self) -> CoroGraphUseDef {
        let scopes: HashMap<_, _> = self
            .graph
            .scopes
            .iter()
            .enumerate()
            .map(|(i, scope)| (CoroScopeRef(i), self.analyze_scope(scope)))
            .collect();
        let mut union_uses = scopes
            .iter()
            .fold(AccessTree::new(), |acc, (_, scope_use_def)| {
                acc.union(&scope_use_def.external_uses)
            });
        union_uses.coalesce_whole_access_chains();
        union_uses.dynamic_access_chains_as_whole();
        CoroGraphUseDef { scopes, union_uses }
    }

    pub fn analyze(graph: &'a CoroGraph) -> CoroGraphUseDef {
        Self::new(graph).analyze_graph()
    }
}
