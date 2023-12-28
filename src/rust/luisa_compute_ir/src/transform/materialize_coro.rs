// This file implements the materialization of subroutines in a coroutine. It analyzes the
// input coroutine module, generates the coroutine graph and transfer graph, computes the
// coroutine frame layout, and finally materializes the subroutines into callable modules.
// Some corner cases to consider:
// - Some values might be promoted to values in the coroutine frame and should be loaded
//   before use.
// - Some values might not dominate their uses any more as they might have been moved into
//   a `SkipIfFirst` block. We need to promote them to locals.
// - Some "replayable" values are not included in the coroutine frame, nor defined in the
//   subroutine body. We need to replay them.

use crate::analysis::coro_frame::CoroFrame;
use crate::analysis::coro_graph::{
    CoroGraph, CoroInstrRef, CoroInstruction, CoroScope, CoroScopeRef,
};
use crate::analysis::coro_transfer_graph::CoroTransferGraph;
use crate::analysis::coro_use_def::CoroUseDefAnalysis;
use crate::analysis::replayable_values::ReplayableValueAnalysis;
use crate::analysis::utility::AccessTree;
use crate::ir::{
    new_node, BasicBlock, CallableModule, CallableModuleRef, Const, CurveBasisSet, Func,
    Instruction, IrBuilder, Module, ModuleFlags, ModuleKind, Node, NodeRef, Primitive, SwitchCase,
    Type,
};
use crate::transform::canonicalize_control_flow::CanonicalizeControlFlow;
use crate::transform::defer_load::DeferLoad;
use crate::transform::demote_locals::DemoteLocals;
use crate::transform::Transform;
use crate::{CArc, CBoxedSlice, Pooled};
use bitflags::Flags;
use std::collections::HashMap;

pub(crate) struct MaterializeCoro;

struct CoroScopeMaterializer<'a> {
    frame: &'a CoroFrame<'a>,
    coro: &'a CallableModule,
    token: Option<u32>, // None for entry
    scope: CoroScopeRef,
    args: Vec<NodeRef>,
}

impl<'a> CoroScopeMaterializer<'a> {
    fn get_frame_node(&self) -> NodeRef {
        self.args[0].clone()
    }

    fn get_scope(&self) -> &CoroScope {
        &self.frame.graph.get_scope(self.scope)
    }

    fn get_instr(&self, instr: CoroInstrRef) -> &CoroInstruction {
        self.frame.graph.get_instr(instr)
    }

    fn create_args(&self) -> Vec<NodeRef> {
        let mut args = Vec::new();
        for (i, arg) in self.coro.args.iter().enumerate() {
            if i == 0 {
                // the coro frame
                let node = new_node(
                    &self.coro.pools,
                    Node::new(
                        CArc::new(Instruction::Argument { by_value: false }),
                        self.frame.interface_type.clone(),
                    ),
                );
                args.push(node);
            } else {
                // normal args
                let instr = &arg.get().instruction;
                match instr.as_ref() {
                    Instruction::Buffer
                    | Instruction::Bindless
                    | Instruction::Texture2D
                    | Instruction::Texture3D
                    | Instruction::Accel
                    | Instruction::Argument { .. } => {
                        let node = new_node(
                            &self.coro.pools,
                            Node::new(instr.clone(), arg.type_().clone()),
                        );
                        args.push(node);
                    }
                    _ => unreachable!("Invalid argument type"),
                }
            }
        }
        args
    }

    fn new(frame: &'a CoroFrame<'a>, coro: &'a CallableModule, token: Option<u32>) -> Self {
        let scope = if let Some(token) = token {
            frame.graph.tokens[&token]
        } else {
            frame.graph.entry
        };
        let mut m = Self {
            frame,
            coro,
            token,
            scope,
            args: Vec::new(),
        };
        m.args = m.create_args();
        m
    }
}

struct CoroScopeMaterializerCtx {
    mappings: HashMap<NodeRef, NodeRef>, // mapping from old nodes to new nodes
    entry_builder: IrBuilder,            // suitable for declaring locals
    first_flag: Option<NodeRef>,
    uses_ray_tracing: bool,
    replayable: ReplayableValueAnalysis,
}

struct CoroScopeMaterializerState {
    builder: IrBuilder, // current running build
}

impl CoroScopeMaterializerState {
    fn clone_for_branch_block(&self) -> Self {
        Self {
            builder: IrBuilder::new(self.builder.pools.clone()),
        }
    }
}

impl<'a> CoroScopeMaterializer<'a> {
    fn resume(&self, ctx: &mut CoroScopeMaterializerCtx) {
        let mappings = self
            .frame
            .resume(self.scope, self.get_frame_node(), &mut ctx.entry_builder);
        for (old_node, new_node) in mappings {
            ctx.mappings.insert(old_node, new_node);
        }
    }

    fn suspend(&self, target: u32, ctx: &mut CoroScopeMaterializerCtx, b: &mut IrBuilder) {
        self.frame.suspend(
            self.scope,
            target,
            self.get_frame_node(),
            b,
            &mut ctx.mappings,
        );
    }

    fn terminate(&self, b: &mut IrBuilder) {
        self.frame.terminate(self.scope, self.get_frame_node(), b);
    }

    fn ref_or_local(
        &self,
        old_node: NodeRef,
        ctx: &mut CoroScopeMaterializerCtx,
        state: &mut CoroScopeMaterializerState,
    ) -> NodeRef {
        if let Some(defined) = ctx.mappings.get(&old_node) {
            defined.clone()
        } else if old_node.is_gep() {
            let (root, chain) = AccessTree::access_chain_from_gep_chain(old_node);
            let chain: Vec<_> = chain
                .iter()
                .map(|node| self.value_or_load(node.clone(), ctx, state))
                .collect();
            let root = self.ref_or_local(root, ctx, state);
            let gep = state
                .builder
                .gep(root, chain.as_slice(), old_node.type_().clone());
            ctx.mappings.insert(old_node, gep.clone());
            gep
        } else {
            // not defined yet, we'll define it now
            let local = ctx.entry_builder.local_zero_init(old_node.type_().clone());
            ctx.mappings.insert(old_node.clone(), local.clone());
            local
        }
    }

    fn replay_value(&self, old_node: NodeRef, ctx: &mut CoroScopeMaterializerCtx) -> NodeRef {
        if let Some(replayed) = ctx.mappings.get(&old_node) {
            return replayed.clone();
        }
        match old_node.get().instruction.as_ref() {
            Instruction::Const(c) => ctx.entry_builder.const_(c.clone()),
            Instruction::Call(func, args) => match func {
                Func::Unreachable(_)
                | Func::ZeroInitializer
                | Func::ThreadId
                | Func::BlockId
                | Func::WarpSize
                | Func::WarpLaneId
                | Func::DispatchId
                | Func::DispatchSize => {
                    ctx.entry_builder
                        .call(func.clone(), &[], old_node.type_().clone())
                }
                Func::CoroId => self
                    .frame
                    .read_coro_id(self.get_frame_node(), &mut ctx.entry_builder),
                Func::CoroToken => ctx
                    .entry_builder
                    .const_(Const::Uint32(self.token.unwrap_or(0))),
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
                | Func::GetElementPtr
                | Func::Struct
                | Func::Array
                | Func::Mat
                | Func::Mat2
                | Func::Mat3
                | Func::Mat4 => {
                    let replayed_args: Vec<_> = args
                        .iter()
                        .map(|arg| self.replay_value(arg.clone(), ctx))
                        .collect();
                    ctx.entry_builder.call(
                        func.clone(),
                        replayed_args.as_slice(),
                        old_node.type_().clone(),
                    )
                }
                _ => unreachable!("non-replayable value"),
            },
            _ => unreachable!("non-replayable value"),
        }
    }

    fn try_replay(&self, old_node: NodeRef, ctx: &mut CoroScopeMaterializerCtx) -> Option<NodeRef> {
        if let Some(defined) = ctx.mappings.get(&old_node) {
            // if already defined, simply return it
            Some(defined.clone())
        } else if !ctx.replayable.detect(old_node) {
            None
        } else {
            let replayed = self.replay_value(old_node, ctx);
            ctx.mappings.insert(old_node, replayed.clone());
            Some(replayed)
        }
    }

    fn value_or_load(
        &self,
        old_node: NodeRef,
        ctx: &mut CoroScopeMaterializerCtx,
        state: &mut CoroScopeMaterializerState,
    ) -> NodeRef {
        if let Some(node) = self.try_replay(old_node, ctx) {
            node
        } else {
            let node = self.ref_or_local(old_node, ctx, state);
            if node.is_local() || node.is_gep() {
                state.builder.load(node)
            } else {
                node
            }
        }
    }

    fn def_or_assign(
        &self,
        old_node: NodeRef,
        new_value: NodeRef,
        ctx: &mut CoroScopeMaterializerCtx,
        state: &mut CoroScopeMaterializerState,
    ) {
        let var = self.ref_or_local(old_node, ctx, state);
        state.builder.update(var, new_value);
    }

    fn materialize_branch_block(
        &self,
        block: &Vec<CoroInstrRef>,
        ctx: &mut CoroScopeMaterializerCtx,
        state: &CoroScopeMaterializerState,
    ) -> Pooled<BasicBlock> {
        let mut branch_state = state.clone_for_branch_block();
        self.materialize_instructions(block.as_slice(), ctx, &mut branch_state);
        branch_state.builder.finish()
    }

    fn make_first_flag(&self, ctx: &mut CoroScopeMaterializerCtx) {
        assert_eq!(ctx.first_flag, None, "First flag already defined");
        let flag = {
            let b = &mut ctx.entry_builder;
            b.comment(CBoxedSlice::from("make first flag".as_bytes()));
            let v = b.const_(Const::Bool(false));
            b.local(v)
        };
        ctx.first_flag = Some(flag);
    }

    fn materialize_call(
        &self,
        ret: NodeRef,
        func: Func,
        args: &[NodeRef],
        ctx: &mut CoroScopeMaterializerCtx,
        state: &mut CoroScopeMaterializerState,
    ) {
        if ctx.mappings.contains_key(&ret) {
            return;
        }
        macro_rules! process_return {
            ($call: expr) => {
                match $call.type_().as_ref() {
                    Type::Void => { /* nothing */ }
                    Type::UserData => todo!(),
                    Type::Opaque(_) => {
                        // as non-copyable reference
                        ctx.mappings.insert(ret.clone(), $call);
                    }
                    _ => self.def_or_assign(ret.clone(), $call, ctx, state),
                }
            };
        }
        match func {
            // callable
            Func::Callable(c) => {
                let args: Vec<_> =
                    c.0.args
                        .iter()
                        .zip(args.iter())
                        .map(|(formal, &given)| {
                            if formal.is_reference_argument() || formal.type_().is_opaque("") {
                                self.ref_or_local(given, ctx, state)
                            } else {
                                self.value_or_load(given, ctx, state)
                            }
                        })
                        .collect();
                let call = state.builder.call(
                    Func::Callable(c.clone()),
                    args.as_slice(),
                    ret.type_().clone(),
                );
                process_return!(call)
            }
            // always replayable functions, should not appear here
            Func::CoroId
            | Func::CoroToken
            | Func::ZeroInitializer
            | Func::Unreachable(_)
            | Func::ThreadId
            | Func::BlockId
            | Func::WarpSize
            | Func::WarpLaneId
            | Func::DispatchId
            | Func::DispatchSize => unreachable!(),
            // local variable operations
            Func::Load => {
                let loaded = self.value_or_load(args[0].clone(), ctx, state);
                self.def_or_assign(ret, loaded, ctx, state);
            }
            Func::AddressOf => {
                // the first argument should be reference
                let var = self.ref_or_local(args[0].clone(), ctx, state);
                let addr = state
                    .builder
                    .call(func.clone(), &[var], ret.type_().clone());
                self.def_or_assign(ret, addr, ctx, state);
            }
            Func::GetElementPtr => {
                let (root, chain) = AccessTree::access_chain_from_gep_chain(ret);
                let root = self.ref_or_local(root, ctx, state);
                let chain: Vec<_> = chain
                    .iter()
                    .map(|&i| self.value_or_load(i, ctx, state))
                    .collect();
                let gep = state
                    .builder
                    .gep(root, chain.as_slice(), ret.type_().clone());
                ctx.mappings.insert(ret, gep);
            }
            // AD functions
            Func::PropagateGrad => todo!(),
            Func::OutputGrad => todo!(),
            Func::RequiresGradient => todo!(),
            Func::Backward => todo!(),
            Func::Gradient => todo!(),
            Func::GradientMarker => todo!(),
            Func::AccGrad => todo!(),
            Func::Detach => todo!(),
            // resource functions, the first argument should always be a reference
            Func::RayTracingQueryAll
            | Func::RayTracingQueryAny
            | Func::RayTracingInstanceTransform
            | Func::RayTracingInstanceVisibilityMask
            | Func::RayTracingInstanceUserId
            | Func::RayTracingSetInstanceTransform
            | Func::RayTracingSetInstanceOpacity
            | Func::RayTracingSetInstanceVisibility
            | Func::RayTracingSetInstanceUserId
            | Func::RayTracingTraceClosest
            | Func::RayTracingTraceAny
            | Func::RayQueryWorldSpaceRay
            | Func::RayQueryProceduralCandidateHit
            | Func::RayQueryTriangleCandidateHit
            | Func::RayQueryCommittedHit
            | Func::RayQueryCommitTriangle
            | Func::RayQueryCommitProcedural
            | Func::RayQueryTerminate
            | Func::IndirectDispatchSetCount
            | Func::IndirectDispatchSetKernel
            | Func::AtomicRef
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
            | Func::BufferWrite
            | Func::BufferSize
            | Func::BufferAddress
            | Func::ByteBufferRead
            | Func::ByteBufferWrite
            | Func::ByteBufferSize
            | Func::Texture2dRead
            | Func::Texture2dWrite
            | Func::Texture2dSize
            | Func::Texture3dRead
            | Func::Texture3dWrite
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
            | Func::BindlessBufferWrite
            | Func::BindlessBufferSize
            | Func::BindlessBufferAddress
            | Func::BindlessBufferType
            | Func::BindlessByteBufferRead => {
                let args: Vec<_> = args
                    .iter()
                    .enumerate()
                    .map(|(i, &a)| {
                        if i == 0 {
                            // resource
                            self.ref_or_local(a, ctx, state)
                        } else {
                            // value
                            self.value_or_load(a, ctx, state)
                        }
                    })
                    .collect();
                let call = state
                    .builder
                    .call(func.clone(), args.as_slice(), ret.type_().clone());
                process_return!(call)
            }
            // functions with all value arguments
            Func::Assume
            | Func::Assert(_)
            | Func::RasterDiscard
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
            | Func::SynchronizeBlock
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
            | Func::ShaderExecutionReorder
            | Func::CpuCustomOp(_) => {
                let args: Vec<_> = args
                    .iter()
                    .map(|&a| self.value_or_load(a, ctx, state))
                    .collect();
                let call = state
                    .builder
                    .call(func.clone(), args.as_slice(), ret.type_().clone());
                process_return!(call)
            }
            // other, unused
            Func::Unknown0 => todo!(),
            Func::Unknown1 => todo!(),
        }
    }

    fn materialize_simple(
        &self,
        node: NodeRef,
        ctx: &mut CoroScopeMaterializerCtx,
        state: &mut CoroScopeMaterializerState,
    ) {
        if ctx.replayable.detect(node) {
            self.replay_value(node.clone(), ctx);
            return;
        }
        match node.get().instruction.as_ref() {
            Instruction::Local { init } => {
                let init = self.value_or_load(init.clone(), ctx, state);
                let this = self.ref_or_local(node, ctx, state);
                state.builder.update(this, init);
            }
            Instruction::Update { var, value } => {
                let value = self.value_or_load(value.clone(), ctx, state);
                self.def_or_assign(var.clone(), value, ctx, state);
            }
            Instruction::Call(func, args) => {
                self.materialize_call(node, func.clone(), args.iter().as_slice(), ctx, state);
            }
            Instruction::Loop { body, cond } => {
                let mut body_state = state.clone_for_branch_block();
                for node in body.iter() {
                    self.materialize_simple(node, ctx, &mut body_state);
                }
                let cond = self.value_or_load(cond.clone(), ctx, &mut body_state);
                let body = body_state.builder.finish();
                state.builder.loop_(body, cond);
            }
            Instruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                let cond = self.value_or_load(cond.clone(), ctx, state);
                let true_branch = self.materialize_branch_in_simple(true_branch, ctx, state);
                let false_branch = self.materialize_branch_in_simple(false_branch, ctx, state);
                state.builder.if_(cond, true_branch, false_branch);
            }
            Instruction::Switch {
                value,
                cases,
                default,
            } => {
                let value = self.value_or_load(value.clone(), ctx, state);
                let cases: Vec<_> = cases
                    .iter()
                    .map(|case| SwitchCase {
                        value: case.value,
                        block: self.materialize_branch_in_simple(&case.block, ctx, state),
                    })
                    .collect();
                let default = self.materialize_branch_in_simple(default, ctx, state);
                state.builder.switch(value, cases.as_slice(), default);
            }
            Instruction::AdScope {
                body,
                n_forward_grads,
                forward,
            } => {
                let body = self.materialize_branch_in_simple(body, ctx, state);
                if *forward {
                    state.builder.fwd_ad_scope(body, *n_forward_grads);
                } else {
                    state.builder.ad_scope(body);
                }
            }
            Instruction::RayQuery {
                ray_query,
                on_triangle_hit,
                on_procedural_hit,
            } => {
                let ray_query = self.ref_or_local(ray_query.clone(), ctx, state);
                let on_triangle_hit =
                    self.materialize_branch_in_simple(on_triangle_hit, ctx, state);
                let on_procedural_hit =
                    self.materialize_branch_in_simple(on_procedural_hit, ctx, state);
                ctx.uses_ray_tracing = true;
                state.builder.ray_query(
                    ray_query,
                    on_triangle_hit,
                    on_procedural_hit,
                    node.type_().clone(),
                );
            }
            Instruction::Print { fmt, args } => {
                let args: Vec<_> = args
                    .iter()
                    .map(|arg| self.value_or_load(arg.clone(), ctx, state))
                    .collect();
                state.builder.print(fmt.clone(), args.as_slice());
            }
            Instruction::AdDetach(body) => {
                let body = self.materialize_branch_in_simple(body.as_ref(), ctx, state);
                state.builder.ad_detach(body);
            }
            Instruction::Comment(msg) => {
                state.builder.comment(msg.clone());
            }
            Instruction::CoroRegister { var, value, token } => {
                let value = self.value_or_load(value.clone(), ctx, state);
                todo!();
            }
            _ => unreachable!(),
        }
    }

    fn materialize_branch_in_simple(
        &self,
        block: &BasicBlock,
        ctx: &mut CoroScopeMaterializerCtx,
        state: &CoroScopeMaterializerState,
    ) -> Pooled<BasicBlock> {
        let mut branch_state = state.clone_for_branch_block();
        for node in block.iter() {
            self.materialize_simple(node, ctx, &mut branch_state);
        }
        branch_state.builder.finish()
    }

    fn materialize_instr(
        &self,
        instr: &CoroInstruction,
        ctx: &mut CoroScopeMaterializerCtx,
        state: &mut CoroScopeMaterializerState,
    ) {
        match instr {
            CoroInstruction::Simple(node) => {
                self.materialize_simple(node.clone(), ctx, state);
            }
            CoroInstruction::ConditionStackReplay { items } => {
                for item in items.iter() {
                    macro_rules! decode_value {
                        ($t:tt, $value: expr) => {
                            state.builder.const_(Const::$t($value))
                        };
                    }
                    let value = match item.node.type_().as_ref() {
                        Type::Primitive(p) => match p {
                            Primitive::Bool => decode_value!(Bool, item.value != 0),
                            Primitive::Int8 => decode_value!(Int8, item.value as i8),
                            Primitive::Uint8 => decode_value!(Uint8, item.value as u8),
                            Primitive::Int16 => decode_value!(Int16, item.value as i16),
                            Primitive::Uint16 => decode_value!(Uint16, item.value as u16),
                            Primitive::Int32 => decode_value!(Int32, item.value),
                            Primitive::Uint32 => decode_value!(Uint32, item.value as u32),
                            Primitive::Int64 => decode_value!(Int64, item.value as i64),
                            Primitive::Uint64 => decode_value!(Uint64, item.value as u64),
                            _ => unreachable!(),
                        },
                        _ => unreachable!(),
                    };
                    self.def_or_assign(item.node.clone(), value, ctx, state);
                }
            }
            CoroInstruction::MakeFirstFlag => {
                self.make_first_flag(ctx);
            }
            CoroInstruction::SkipIfFirstFlag { body, .. } => {
                let flag = state.builder.load(ctx.first_flag.unwrap().clone());
                let true_branch = self.materialize_branch_block(body, ctx, state);
                let false_branch = IrBuilder::new(state.builder.pools.clone()).finish();
                state.builder.if_(flag, true_branch, false_branch);
            }
            CoroInstruction::ClearFirstFlag(_) => {
                let v = state.builder.const_(Const::Bool(true));
                state.builder.update(ctx.first_flag.unwrap(), v);
            }
            CoroInstruction::Loop { body, cond } => {
                // note: cond is inside the scope of body, so we have to convert it before pop
                let mut body_state = state.clone_for_branch_block();
                self.materialize_instructions(body.as_slice(), ctx, &mut body_state);
                let cond = if let CoroInstruction::Simple(cond) = self.get_instr(*cond) {
                    self.value_or_load(*cond, ctx, &mut body_state)
                } else {
                    unreachable!()
                };
                // now we can pop the body and build the instruction
                let body = body_state.builder.finish();
                state.builder.loop_(body, cond);
            }
            CoroInstruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                let cond = if let CoroInstruction::Simple(cond) = self.get_instr(*cond) {
                    self.value_or_load(cond.clone(), ctx, state)
                } else {
                    unreachable!()
                };
                let true_branch = self.materialize_branch_block(true_branch, ctx, state);
                let false_branch = self.materialize_branch_block(false_branch, ctx, state);
                state.builder.if_(cond, true_branch, false_branch);
            }
            CoroInstruction::Switch {
                cond,
                cases,
                default,
            } => {
                let cond = if let CoroInstruction::Simple(cond) = self.get_instr(*cond) {
                    self.value_or_load(cond.clone(), ctx, state)
                } else {
                    unreachable!()
                };
                let cases: Vec<_> = cases
                    .iter()
                    .map(|case| SwitchCase {
                        value: case.value,
                        block: self.materialize_branch_block(&case.body, ctx, state),
                    })
                    .collect();
                let default = self.materialize_branch_block(default, ctx, state);
                state.builder.switch(cond, cases.as_slice(), default);
            }
            CoroInstruction::Suspend { token } => self.suspend(*token, ctx, &mut state.builder),
            CoroInstruction::Terminate => self.terminate(&mut state.builder),
            _ => unreachable!(),
        }
    }

    fn materialize_instructions(
        &self,
        instructions: &[CoroInstrRef],
        ctx: &mut CoroScopeMaterializerCtx,
        state: &mut CoroScopeMaterializerState,
    ) {
        for &instr in instructions {
            self.materialize_instr(self.get_instr(instr), ctx, state);
        }
    }

    fn materialize(&self) -> CallableModule {
        let mappings: HashMap<_, _> = self
            .coro
            .args
            .iter()
            .cloned()
            .zip(self.args.iter().cloned())
            .collect();
        let mut entry_builder = IrBuilder::new(self.coro.pools.clone());
        let mut ctx = CoroScopeMaterializerCtx {
            mappings,
            entry_builder,
            first_flag: None,
            uses_ray_tracing: false,
            replayable: ReplayableValueAnalysis::new(false),
        };
        // resume states and generate first flag if not entry
        if let Some(_) = self.token {
            self.resume(&mut ctx);
        }
        // materialize the body
        let mut b = IrBuilder::new_without_bb(self.coro.pools.clone());
        b.set_insert_point(ctx.entry_builder.get_insert_point());
        b.comment(CBoxedSlice::from(format!(
            "coro body (token = {})",
            self.token.unwrap_or(0)
        )));
        let mut state = CoroScopeMaterializerState { builder: b };
        self.materialize_instructions(&self.get_scope().instructions, &mut ctx, &mut state);
        // create the callable module
        CallableModule {
            module: Module {
                kind: ModuleKind::Function,
                entry: ctx.entry_builder.finish(),
                flags: ModuleFlags::empty(),
                curve_basis_set: if ctx.uses_ray_tracing {
                    self.coro.module.curve_basis_set
                } else {
                    CurveBasisSet::empty()
                },
                pools: self.coro.pools.clone(),
            },
            ret_type: Type::void(),
            args: CBoxedSlice::new(self.args.clone()),
            captures: CBoxedSlice::new(Vec::new()),
            subroutines: CBoxedSlice::new(Vec::new()),
            subroutine_ids: CBoxedSlice::new(Vec::new()),
            cpu_custom_ops: CBoxedSlice::new(Vec::new()),
            pools: self.coro.pools.clone(),
        }
    }
}

impl Transform for MaterializeCoro {
    fn transform_callable(&self, callable: CallableModule) -> CallableModule {
        let callable = CanonicalizeControlFlow.transform_callable(callable);
        let callable = DemoteLocals.transform_callable(callable);
        let callable = DeferLoad.transform_callable(callable);
        let coro_graph = CoroGraph::from(&callable.module);
        coro_graph.dump();
        let coro_use_def = CoroUseDefAnalysis::analyze(&coro_graph);
        coro_use_def.dump();
        let coro_transfer_graph = CoroTransferGraph::build(&coro_graph, &coro_use_def);
        coro_transfer_graph.dump();
        let coro_frame = CoroFrame::build(&coro_graph, &coro_transfer_graph);
        coro_frame.dump();
        let mut entry = CoroScopeMaterializer::new(&coro_frame, &callable, None).materialize();
        let subroutines: Vec<_> = coro_graph
            .tokens
            .keys()
            .map(|token| {
                let r =
                    CoroScopeMaterializer::new(&coro_frame, &callable, Some(*token)).materialize();
                CallableModuleRef(CArc::new(r))
            })
            .collect();
        let subroutine_token: Vec<_> = coro_graph.tokens.keys().copied().collect();
        entry.subroutines = CBoxedSlice::new(subroutines);
        entry.subroutine_ids = CBoxedSlice::new(subroutine_token);
        entry
    }
}
