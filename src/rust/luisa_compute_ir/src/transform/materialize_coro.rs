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
use crate::ir::{
    new_node, CallableModule, CallableModuleRef, Instruction, IrBuilder, Module, ModuleFlags,
    ModuleKind, Node, NodeRef, Type,
};
use crate::transform::canonicalize_control_flow::CanonicalizeControlFlow;
use crate::transform::defer_load::DeferLoad;
use crate::transform::demote_locals::DemoteLocals;
use crate::transform::Transform;
use crate::{CArc, CBoxedSlice};
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
    mappings: HashMap<NodeRef, NodeRef>,
    entry_builder: IrBuilder,
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

    fn materialize_instr(
        &self,
        instr: &CoroInstruction,
        ctx: &mut CoroScopeMaterializerCtx,
        b: &mut IrBuilder,
    ) {
        // todo!()
    }

    fn materialize_instructions(
        &self,
        instructions: &[CoroInstrRef],
        ctx: &mut CoroScopeMaterializerCtx,
        b: &mut IrBuilder,
    ) {
        for &instr in instructions {
            self.materialize_instr(self.get_instr(instr), ctx, b);
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
        };
        // resume if not entry
        if let Some(_) = self.token {
            self.resume(&mut ctx);
        }
        // materialize the body
        let mut b = IrBuilder::new_without_bb(self.coro.pools.clone());
        b.set_insert_point(ctx.entry_builder.get_insert_point());
        b.comment(CBoxedSlice::from("coro body".to_string()));
        self.materialize_instructions(&self.get_scope().instructions, &mut ctx, &mut b);
        // create the callable module
        CallableModule {
            module: Module {
                kind: ModuleKind::Function,
                entry: ctx.entry_builder.finish(),
                flags: ModuleFlags::empty(),
                curve_basis_set: self.coro.module.curve_basis_set,
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
