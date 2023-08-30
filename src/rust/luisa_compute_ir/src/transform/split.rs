use std::collections::{HashMap, HashSet};
use lazy_static::lazy_static;

use crate::{CArc, CBoxedSlice, Pooled};
use crate::ir::{SwitchCase, Module, Instruction, BasicBlock, KernelModule, IrBuilder, ModulePools, NodeRef, Node, new_node, Type, Capture, INVALID_REF, CallableModule, Func, CallableModuleRef, PhiIncoming};

struct FrameTokenManager {
    frame_token_occupied: HashSet<u32>,
    frame_token_counter: u32,
}

impl FrameTokenManager {
    fn register_frame_token(&mut self, token: u32) {
        self.frame_token_occupied.insert(token);
    }
    fn get_new_token(&mut self) -> u32 {
        while self.frame_token_occupied.contains(&self.frame_token_counter) {
            self.frame_token_counter -= 1;
        }
        self.frame_token_occupied.insert(self.frame_token_counter);
        self.frame_token_counter
    }
    fn get_instance() -> &'static mut Self {
        static mut INSTANCE: FrameTokenManager = FrameTokenManager {
            frame_token_occupied: HashSet::new(),
            frame_token_counter: u32::MAX,
        };
        unsafe { &mut INSTANCE }
    }
}

static mut FRAME_TOKEN_OCCUPIED: HashSet<u32> = HashSet::new();
static mut FRAME_TOKEN_COUNTER: u32 = u32::MAX;

struct ModuleDuplicatorCtx {
    nodes: HashMap<NodeRef, NodeRef>,   // map new node to old node
    blocks: HashMap<*const BasicBlock, Pooled<BasicBlock>>,
}

struct ModuleDuplicator {
    callables: HashMap<*const CallableModule, CArc<CallableModule>>,
    current: Option<ModuleDuplicatorCtx>,
}

struct SplitManager {
    module_duplicators: HashMap<u32, ModuleDuplicator>,
    old2new: HashMap<NodeRef, HashSet<(u32, NodeRef)>>,    // map old node to new node
    coro_frames: Vec<NodeRef>,
}

impl SplitManager {
    fn new() -> Self {
        Self {
            module_duplicators: HashMap::new(),
            old2new: HashMap::new(),
            coro_frames: Vec::new(),
        }
    }

    fn with_context<T, F: FnOnce(&mut Self) -> T>(&mut self, frame_token: u32, f: F) -> T {
        let mut ctx = ModuleDuplicatorCtx {
            nodes: HashMap::new(),
            blocks: HashMap::new(),
        };
        let ret = f(self);
        self.module_duplicators.get_mut(&frame_token).unwrap().current = Some(ctx);
        ret
    }

    // pub unsafe fn a() -> &'static mut i32 {
    //     static mut a_: i32 = 0;
    //     &mut a_
    // }

    /*
    loop {
        BLOCK A;
        split(k);
        BLOCK B;
    } cond(C);


    Design A:
    ----------------------
    |      BLOCK A;      |
    |     suspend(k);    |
    ----------------------
              |
              |<----------------
              |                |
              V                |
    ----------------------     |
    |     resume(k);     |     |
    |      BLOCK B;      |     |
    | if (cond C) {      |     |
    |      BLOCK A;      |     |
    |     suspend(k);    | -----
    | }                  |
    ----------------------

    Design B:
              ------------------
              |                |
              V                |
    ----------------------     |
    |    resume(delta);  |     |
    |      BLOCK A;      |     |
    |     suspend(k);    |     |
    ----------------------     |
              |                |
              |                |
              |                |
              V                |
    ----------------------     |
    |     resume(k);     |     |
    |      BLOCK B;      |     |
    | if (cond C) {      |     |
    |     suspend(k);    | -----
    | }                  |
    ----------------------

     */

    /// return True if there is any split in it
    fn visit_loop(&mut self, body: &Pooled<BasicBlock>, cond: &NodeRef) -> bool {
        let mut split_exist = false;

        split_exist
    }
    fn split(&mut self, kernel: &KernelModule) {
        let bb = &kernel.module.entry;

        // register frame token
        let mut frame_token_manager = FrameTokenManager::get_instance();
        for node_ref_present in bb.iter() {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
                Instruction::CoroSplitMark { token } => {
                    unsafe { frame_token_manager.register_frame_token(*token); }
                }
                _ => {}
            }
        }
        let mut frame_token = unsafe { frame_token_manager.get_new_token() };

        // prepare
        let pools = &kernel.pools;
        let mut builder = IrBuilder::new(pools.clone());



        let mut node_ref_present = bb.first.get().next;
        while node_ref_present != bb.last {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
                // coroutine related instructions
                Instruction::CoroSplitMark { token } => unsafe {
                    // turn CoroSplitMark into CoroSuspend
                    frame_token = *token;
                    builder.coro_suspend(*frame_token);

                    let frame_bb = builder.finish();
                    let frame = Instruction::CoroFrame {
                        token: *frame_token,
                        body: frame_bb,
                    };
                    let frame_node = Node::new(CArc::new(frame), CArc::new(Type::Void));
                    let frame_node_ref = new_node(&pools, frame_node);
                    self.coro_frames.push(frame_node_ref);

                    builder = IrBuilder::new(pools.clone());
                    builder.coro_resume(*frame_token);
                }
                Instruction::CoroSuspend { token } => {
                    // TODO
                    unimplemented!("Split: CoroSuspend")
                }
                Instruction::CoroResume { token } => {
                    // TODO
                    unimplemented!("Split: CoroResume");
                }

                // 3 Instructions to be processed
                Instruction::Loop { body, cond } => {}
                Instruction::If { cond, true_branch, false_branch } => {}
                Instruction::Switch { value, cases, default } => {}

                // other instructions, just deep clone and change the node_ref to new ones
                _ => {
                    builder.append();
                }
            }
            node_ref_present = node_ref_present.get().next;
        }
    }


    // duplicate functions
    fn find_duplicated_block(&self, frame_token: u32, bb: &Pooled<BasicBlock>) -> Pooled<BasicBlock> {
        let ctx = self.module_duplicators.get(&frame_token).unwrap().current.as_ref().unwrap();
        ctx.blocks.get(&bb.as_ptr()).unwrap().clone()
    }
    fn find_duplicated_node(&self, frame_token: u32, node: NodeRef) -> NodeRef {
        if !node.valid() { return INVALID_REF; }
        let ctx = self.module_duplicators.get(&frame_token).unwrap().current.as_ref().unwrap();
        ctx.nodes.get(&node).unwrap().clone()
    }
    fn duplicate_arg(&mut self, frame_token: u32, pools: &CArc<ModulePools>, node_ref: NodeRef) -> NodeRef {
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
            _ => unreachable!("Invalid argument type")
        };
        let dup_node = Node::new(dup_instr, node.type_.clone());
        let dup_node_ref = new_node(pools, dup_node);
        // add to node map
        self.old2new.entry(node_ref).or_default().insert((frame_token, dup_node_ref));
        self.module_duplicators.get_mut(&frame_token).unwrap()
            .current.as_mut().unwrap().nodes.insert(node_ref, dup_node_ref);
        dup_node_ref
    }
    fn duplicate_args(&mut self, frame_token: u32, pools: &CArc<ModulePools>,
                      args: &CBoxedSlice<NodeRef>) -> CBoxedSlice<NodeRef> {
        let dup_args: Vec<NodeRef> = args.iter().map(|arg| {
            self.duplicate_arg(frame_token, pools, arg.clone())
        }).collect();
        CBoxedSlice::new(dup_args)
    }
    fn duplicate_captures(&mut self, frame_token: u32, pools: &CArc<ModulePools>,
                          captures: &CBoxedSlice<Capture>) -> CBoxedSlice<Capture> {
        let dup_captures: Vec<Capture> = captures.iter().map(|capture| {
            Capture {
                node: self.duplicate_arg(frame_token, pools, capture.node.clone()),
                binding: capture.binding.clone(),
            }
        }).collect();
        CBoxedSlice::new(dup_captures)
    }
    fn duplicate_shared(&mut self, frame_token: u32, pools: &CArc<ModulePools>,
                        shared: &CBoxedSlice<NodeRef>) -> CBoxedSlice<NodeRef> {
        let dup_shared: Vec<NodeRef> = shared.iter().map(|shared| {
            self.duplicate_arg(frame_token, pools, shared.clone())
        }).collect();
        CBoxedSlice::new(dup_shared)
    }

    fn duplicate_callable(&mut self, frame_token: u32, callable: &CArc<CallableModule>) -> CArc<CallableModule> {
        if let Some(copy) = self.module_duplicators.get(&frame_token).unwrap()
            .callables.get(&callable.as_ptr()) {
            return copy.clone();
        }
        let dup_callable = self.with_context(frame_token, |this| {
            let dup_args = this.duplicate_args(frame_token, &callable.pools, &callable.args);
            let dup_captures = this.duplicate_captures(frame_token, &callable.pools, &callable.captures);
            let dup_module = this.duplicate_module(&callable.module);
            CallableModule {
                module: dup_module,
                ret_type: callable.ret_type.clone(),
                args: dup_args,
                captures: dup_captures,
                cpu_custom_ops: callable.cpu_custom_ops.clone(),
                pools: callable.pools.clone(),
            }
        });
        let dup_callable = CArc::new(dup_callable);
        self.module_duplicators.get_mut(&frame_token).unwrap().callables.insert(callable.as_ptr(), dup_callable.clone());
        dup_callable
    }

    fn duplicate_node(&mut self, frame_token: u32, builder: &mut IrBuilder, node_ref: NodeRef) -> NodeRef {
        if !node_ref.valid() { return INVALID_REF; }
        let node = node_ref.get();
        assert!(!self.current.as_ref().unwrap().nodes.contains_key(&node_ref),
                "Node {:?} has already been duplicated", node);
        let dup_node = match node.instruction.as_ref() {
            Instruction::Buffer => unreachable!("Buffer should be handled by duplicate_args"),
            Instruction::Bindless => unreachable!("Bindless should be handled by duplicate_args"),
            Instruction::Texture2D => unreachable!("Texture2D should be handled by duplicate_args"),
            Instruction::Texture3D => unreachable!("Texture3D should be handled by duplicate_args"),
            Instruction::Accel => unreachable!("Accel should be handled by duplicate_args"),
            Instruction::Shared => unreachable!("Shared should be handled by duplicate_shared"),
            Instruction::Uniform => unreachable!("Uniform should be handled by duplicate_args"),
            Instruction::Argument { .. } => unreachable!("Argument should be handled by duplicate_args"),
            Instruction::Local { init } => {
                // TODO
                // let dup_init = self.find_duplicated_node(frame, *init);
                // builder.local(dup_init)
                unimplemented!("Split: duplicate local")
            }
            Instruction::UserData(data) => builder.userdata(data.clone()),
            Instruction::Invalid => unreachable!("Invalid node should not appear in non-sentinel nodes"),
            Instruction::Const(const_) => builder.const_(const_.clone()),
            Instruction::Update { var, value } => {
                // TODO: unreachable if SSA
                // let dup_var = self.find_duplicated_node(*var);
                // let dup_value = self.find_duplicated_node(*value);
                // builder.update(dup_var, dup_value)
                unimplemented!("Split: update should not appear if SSA")
            }
            Instruction::Call(func, args) => {
                let dup_func = match func {
                    Func::Callable(callable) => {
                        // TODO
                        // let dup_callable = self.duplicate_callable(&callable.0);
                        // Func::Callable(CallableModuleRef(dup_callable))
                        unimplemented!("Split: duplicate callable");
                    }
                    _ => func.clone()
                };
                let dup_args: Vec<_> = args.iter().map(|arg| {
                    let dup_args = self.find_duplicated_node(frame_token, *arg);
                    dup_args
                }).collect();
                builder.call(dup_func, dup_args.as_slice(), node.type_.clone())
            }
            Instruction::Phi(incomings) => {
                let dup_incomings: Vec<_> = incomings.iter().map(|incoming| {
                    let dup_block = self.find_duplicated_block(frame_token, &incoming.block);
                    let dup_value = self.find_duplicated_node(frame_token, incoming.value);
                    PhiIncoming {
                        value: dup_value,
                        block: dup_block,
                    }
                }).collect();
                builder.phi(dup_incomings.as_slice(), node.type_.clone())
            }
            Instruction::Return(value) => {
                let dup_value = self.find_duplicated_node(frame_token, *value);
                builder.return_(dup_value)
            }
            Instruction::Loop { body, cond } => {
                let dup_body = self.duplicate_block(&builder.pools, body);
                let dup_cond = self.find_duplicated_node(frame_token, *cond);
                builder.loop_(dup_body, dup_cond)
            }
            Instruction::GenericLoop { prepare, cond, body, update } => {
                let dup_prepare = self.duplicate_block(&builder.pools, prepare);
                let dup_body = self.duplicate_block(&builder.pools, body);
                let dup_update = self.duplicate_block(&builder.pools, update);
                let dup_cond = self.find_duplicated_node(frame_token, *cond);
                builder.generic_loop(dup_prepare, dup_cond, dup_body, dup_update)
            }
            Instruction::Break => builder.break_(),
            Instruction::Continue => builder.continue_(),
            Instruction::If { cond, true_branch, false_branch } => {
                let dup_cond = self.find_duplicated_node(frame_token, *cond);
                let dup_true_branch = self.duplicate_block(&builder.pools, true_branch);
                let dup_false_branch = self.duplicate_block(&builder.pools, false_branch);
                builder.if_(dup_cond, dup_true_branch, dup_false_branch)
            }
            Instruction::Switch { value, cases, default } => {
                let dup_value = self.find_duplicated_node(frame_token, *value);
                let dup_cases: Vec<_> = cases.iter().map(|case| {
                    let dup_block = self.duplicate_block(&builder.pools, &case.block);
                    SwitchCase {
                        value: case.value,
                        block: dup_block,
                    }
                }).collect();
                let dup_default = self.duplicate_block(&builder.pools, default);
                builder.switch(dup_value, dup_cases.as_slice(), dup_default)
            }
            Instruction::AdScope { body } => {
                let dup_body = self.duplicate_block(&builder.pools, body);
                builder.ad_scope(dup_body)
            }
            Instruction::RayQuery { ray_query, on_triangle_hit, on_procedural_hit } => {
                let dup_ray_query = self.find_duplicated_node(frame_token, *ray_query);
                let dup_on_triangle_hit = self.duplicate_block(&builder.pools, on_triangle_hit);
                let dup_on_procedural_hit = self.duplicate_block(&builder.pools, on_procedural_hit);
                builder.ray_query(dup_ray_query, dup_on_triangle_hit, dup_on_procedural_hit, node.type_.clone())
            }
            Instruction::AdDetach(body) => {
                let dup_body = self.duplicate_block(&builder.pools, body);
                builder.ad_detach(dup_body)
            }
            Instruction::Comment(msg) => builder.comment(msg.clone()),
        };
        // insert the duplicated node into the map
        self.old2new.entry(node_ref).or_default().insert((frame_token, dup_node));
        self.module_duplicators.get_mut(&frame_token).unwrap().current.as_mut().unwrap().nodes.insert(node_ref, dup_node);
        dup_node
    }
}