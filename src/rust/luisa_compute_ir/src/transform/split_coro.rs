use crate::analysis::coro_frame_v4::{CoroFrameAnalyser, CoroFrameAnalysis};
use std::collections::HashMap;
// use crate::analysis::coro_graph::CoroPreliminaryGraph;
use crate::analysis::coro_graph::CoroGraph;
use crate::analysis::coro_transfer_graph::CoroTransferGraph;
use crate::analysis::coro_use_def::CoroUseDefAnalysis;
use crate::ir::{
    new_node, BasicBlock, CallableModule, Capture, Instruction, IrBuilder, Module, ModulePools,
    Node, NodeRef,
};
use crate::transform::Transform;
use crate::{CArc, CBoxedSlice, Pooled};

use crate::analysis::coro_frame::CoroFrame;

struct ScopeBuilder {
    token: u32,
    builder: IrBuilder,
}

impl ScopeBuilder {
    fn new(frame_token: u32, pools: CArc<ModulePools>) -> Self {
        Self {
            token: frame_token,
            builder: IrBuilder::new(pools),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct CallableModuleInfo {
    args: Vec<NodeRef>,
    captures: Vec<Capture>,
    frame_node: NodeRef,
    old2frame_index: HashMap<NodeRef, usize>,
    register_var2index: HashMap<u32, usize>,
}

#[derive(Default)]
struct Old2NewMap {
    callables: HashMap<*const CallableModule, HashMap<u32, CArc<CallableModule>>>,
    nodes: HashMap<NodeRef, HashMap<u32, NodeRef>>,
    blocks: HashMap<*const BasicBlock, HashMap<u32, Pooled<BasicBlock>>>,
}

#[derive(Default)]
struct New2OldMap {
    callables: HashMap<*const CallableModule, *const CallableModule>,
    nodes: HashMap<NodeRef, NodeRef>,
    blocks: HashMap<*const BasicBlock, *const BasicBlock>,
}

struct SplitManager {
    callable: CallableModule,
    graph: CoroGraph,
    frame_analyser: CoroFrameAnalyser,

    new2old: New2OldMap,
    old2new: Old2NewMap,

    coro_builder: HashMap<u32, IrBuilder>,
    coro_callable_info: HashMap<u32, CallableModuleInfo>,
}

impl SplitManager {
    fn split(
        callable: CallableModule,
        graph: CoroGraph,
        frame_analyser: CoroFrameAnalyser,
    ) -> CallableModule {
        let mut manager = Self {
            callable,
            graph,
            frame_analyser,
            new2old: New2OldMap::default(),
            old2new: Old2NewMap::default(),
            coro_builder: HashMap::new(),
            coro_callable_info: HashMap::new(),
        };
        manager.pre_process();

        todo!()
    }

    fn pre_process(&mut self) {
        let token_vec = self.frame_analyser.token_vec();
        let mut coro_builder = HashMap::new();
        let mut coro_callable_info = HashMap::new();
        let captures = self.callable.captures.clone();
        let args = self.callable.args.clone();
        for token in token_vec.iter() {
            let args = self.duplicate_args(*token, &args);
            let captures = self.duplicate_captures(*token, &captures);
            // args[0] is frame by default
            // replace the type of frame
            args[0].get_mut().type_ = self.frame_analyser.frame_type.clone();
            let mut callable_info = CallableModuleInfo::default();
            callable_info.frame_node = args[0].clone();
            callable_info.args = args;
            callable_info.captures = captures;
            // TODO
            callable_info.old2frame_index = self.frame_analyser.node2frame_slot.clone();

            coro_builder.insert(*token, IrBuilder::new(self.callable.pools.clone()));
            coro_callable_info.insert(*token, callable_info);
        }
        self.coro_builder = coro_builder;
        self.coro_callable_info = coro_callable_info;
    }

    fn duplicate_arg(&mut self, frame_token: u32, node_ref: NodeRef) -> NodeRef {
        let node = node_ref.get();
        let instr = &node.instruction;
        let dup_instr = match instr.as_ref() {
            Instruction::Buffer => instr.clone(),
            Instruction::Bindless => instr.clone(),
            Instruction::Texture2D => instr.clone(),
            Instruction::Texture3D => instr.clone(),
            Instruction::Accel => instr.clone(),
            Instruction::Shared => instr.clone(),
            Instruction::Uniform => instr.clone(),
            Instruction::Argument { .. } => CArc::new(instr.as_ref().clone()),
            _ => unreachable!("Invalid argument type"),
        };
        let dup_node = Node::new(dup_instr, node.type_.clone());
        let dup_node_ref = new_node(&self.callable.pools, dup_node);

        // add to node map
        self.record_node_mapping(frame_token, node_ref, dup_node_ref);
        dup_node_ref
    }
    fn duplicate_args(&mut self, frame_token: u32, args: &CBoxedSlice<NodeRef>) -> Vec<NodeRef> {
        let dup_args: Vec<NodeRef> = args
            .iter()
            .map(|arg| self.duplicate_arg(frame_token, *arg))
            .collect();
        dup_args
    }
    fn duplicate_capture(&mut self, frame_token: u32, capture: &Capture) -> Capture {
        Capture {
            node: self.duplicate_arg(frame_token, capture.node),
            binding: capture.binding.clone(),
        }
    }
    fn duplicate_captures(
        &mut self,
        frame_token: u32,
        captures: &CBoxedSlice<Capture>,
    ) -> Vec<Capture> {
        let dup_captures: Vec<Capture> = captures
            .iter()
            .map(|capture| self.duplicate_capture(frame_token, capture))
            .collect();
        dup_captures
    }

    fn record_node_mapping(&mut self, frame_token: u32, old: NodeRef, new: NodeRef) {
        let mut old_original = old;
        while let Some(node_ref_t) = self.new2old.nodes.get(&old_original) {
            old_original = node_ref_t.clone();
        }
        self.old2new
            .nodes
            .entry(old_original)
            .or_default()
            .insert(frame_token, new);
        self.new2old.nodes.entry(new).or_insert(old_original);
    }
    fn record_block_mapping(
        &mut self,
        frame_token: u32,
        old: &Pooled<BasicBlock>,
        new: &Pooled<BasicBlock>,
    ) {
        let mut old_original = old.as_ptr();
        while let Some(bb_t) = self.new2old.blocks.get(&old_original) {
            old_original = bb_t.clone();
        }
        self.old2new
            .blocks
            .entry(old_original)
            .or_default()
            .insert(frame_token, new.clone());
        self.new2old
            .blocks
            .entry(new.as_ptr())
            .or_insert(old_original);
    }
    fn record_callable_mapping(
        &mut self,
        frame_token: u32,
        old: &CArc<CallableModule>,
        new: &CArc<CallableModule>,
    ) {
        let mut old_original = old.as_ptr();
        while let Some(callable_t) = self.new2old.callables.get(&old_original) {
            old_original = callable_t.clone();
        }
        self.old2new
            .callables
            .entry(old_original)
            .or_default()
            .insert(frame_token, new.clone());
        self.new2old
            .callables
            .entry(new.as_ptr())
            .or_insert(old_original);
    }
}

pub struct SplitCoro;

impl Transform for SplitCoro {
    fn transform_callable(&self, callable: CallableModule) -> CallableModule {
        println!("SplitCoro::transform_module");
        let graph = CoroGraph::from(&callable.module);
        graph.dump();
        let coro_use_def = CoroUseDefAnalysis::analyze(&graph);
        coro_use_def.dump();
        let transfer_graph = CoroTransferGraph::build(&graph, &coro_use_def);
        transfer_graph.dump();
        let coro_frame = CoroFrame::analyze(&graph, &transfer_graph);
        // TODO
        let frame_analyser = CoroFrameAnalysis::analyse(&graph, &callable);
        SplitManager::split(callable, graph, frame_analyser)
    }
}
