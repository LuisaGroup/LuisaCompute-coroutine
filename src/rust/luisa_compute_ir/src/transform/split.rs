use std::collections::{HashMap, HashSet};
use lazy_static::lazy_static;

use crate::{CArc, CBoxedSlice, Pooled};
use crate::ir::{SwitchCase, Module, Instruction, BasicBlock, KernelModule, IrBuilder, ModulePools, NodeRef, Node, new_node, Type, Capture, INVALID_REF, CallableModule, Func, CallableModuleRef, PhiIncoming};

struct FrameTokenManager {
    frame_token_counter: u32,
    frame_token_occupied: HashSet<u32>,
}
static INVALID_FRAME_TOKEN: u32 = u32::MAX;

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
    fn process_frame_token(&mut self, token: u32) {
        self.frame_token_under_process.insert(token);
    }
    fn test_process_frame_token(&self, token: u32) -> bool {
        self.frame_token_under_process.contains(&token)
    }

    fn get_instance() -> &'static mut Self {
        static mut INSTANCE: FrameTokenManager = FrameTokenManager {
            frame_token_counter: u32::MAX - 1,
            frame_token_occupied: HashSet::new(),
        };
        unsafe { &mut INSTANCE }
    }
}

struct Continuation {
    token: u32,
    prev: HashSet<u32>,
    next: HashSet<u32>,
}

struct FrameBuilder {
    token: u32,
    builder: IrBuilder,
}

struct ModuleDuplicatorCtx {
    nodes: HashMap<NodeRef, NodeRef>,
    // map new node to old node
    blocks: HashMap<*const BasicBlock, Pooled<BasicBlock>>,
}

struct ModuleDuplicator {
    callables: HashMap<*const CallableModule, CArc<CallableModule>>,
    current: Option<ModuleDuplicatorCtx>,
}

struct SplitManager {
    module_duplicators: HashMap<u32, ModuleDuplicator>,
    old2new: HashMap<NodeRef, HashSet<(u32, NodeRef)>>,
    // map old node to new node
    coro_frames: Vec<NodeRef>,
    continuations: HashMap<u32, Continuation>,
    split_in_bb: HashSet<*const BasicBlock>,
}

impl SplitManager {
    fn new() -> Self {
        Self {
            module_duplicators: HashMap::new(),
            old2new: HashMap::new(),
            coro_frames: Vec::new(),
            continuations: HashMap::new(),
            split_in_bb: HashSet::new(),
        }
    }

    fn with_context<T, F: FnOnce(&mut Self) -> T>(&mut self, frame_token: u32, f: F) -> T {
        let mut ctx = ModuleDuplicatorCtx {
            nodes: HashMap::new(),
            blocks: HashMap::new(),
        };
        let ret = f(self);
        self.module_duplicators.get_mut(&frame_token).unwrap_or_default().current = Some(ctx);
        ret
    }

    // pub unsafe fn a() -> &'static mut i32 {
    //     static mut a_: i32 = 0;
    //     &mut a_
    // }

    /*
    BLOCK A;
    loop {
        BLOCK B;
        split(1);
        BLOCK C;
    } cond(cond_1);
    BLOCK D;

    ----------------------
    |      BLOCK A;      |
    |      BLOCK B;      |
    |     suspend(1);    |
    ----------------------
              |
              |<----------------
              |                |
              V                |
    ----------------------     |
    |     resume(1);     |     |
    |      BLOCK C;      |     |
    | if (cond_1 D) {    |     |
    |      BLOCK B;      |     |
    |     suspend(1);    | -----
    | }                  |
    |      BLOCK D;      |
    ----------------------


    BLOCK A;
    if (cond_1) {
        BLOCK B;
        split(1);
        BLOCK C;
    } else {
        BLOCK D;
        split(2);
        BLOCK E;
    }
    BLOCK F;
    if (cond_2) {
        BLOCK G;
        split(3);
        BLOCK H;
    } else {
        BLOCK I;
        split(4);
        BLOCK J;
    }
    BLOCK K;

    ----------------------
    |      BLOCK A;      |
    | if (cond_1) {      |
    |      BLOCK B;      |
    |     suspend(1);    |----
    | } else {           |   |
    |      BLOCK D;      |   |
    |     suspend(2);    |---|-----------
    | }                  |   |          |
    ----------------------   |          |
                             |          |
               ---------------          |
               |                        |
               V                        V
    ----------------------    ----------------------
    |      BLOCK C;      |    |      BLOCK E;      |
    |      BLOCK F;      |    |      BLOCK F;      |
    | if (cond_2) {      |    | if (cond_2) {      |
    |      BLOCK G;      |    |      BLOCK G;      |
    |     suspend(3);    |----|     suspend(3);    |----
    | } else {           |    | } else {           |   |
    |      BLOCK I;      |    |      BLOCK I;      |   |
    |     suspend(4);    |----|     suspend(4);    |---|-----------
    | }                  |    | }                  |   |          |
    ----------------------    ----------------------   |          |
                                                       |          |
               -----------------------------------------          |
               |                        ---------------------------
               |                        |
               V                        V
    ----------------------    ----------------------
    |      BLOCK H;      |    |      BLOCK J;      |
    |      BLOCK K;      |    |      BLOCK K;      |
    ______________________    ______________________

     */


    fn coro_split_mark_in_bb(&mut self, bb: &Pooled<BasicBlock>) -> bool {
        let mut frame_token_manager = FrameTokenManager::get_instance();
        let mut split_in_bb_v = false;
        let record_bb = |bb: &Pooled<BasicBlock>, split_in_bb: bool| {
            if split_in_bb {
                self.split_in_bb.insert(bb.as_ptr());
                split_in_bb_v = true;
            }
        };
        bb.iter().for_each(|node_ref_present| {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
                Instruction::CoroSplitMark { token } => {
                    unsafe { frame_token_manager.register_frame_token(*token); }
                    split_in_bb_v = true;
                }
                // 3 Instructions that might contain coro split mark
                Instruction::Loop { body, cond } => {
                    let split_in_body = self.coro_split_mark_in_bb(body);
                    record_bb(body, split_in_body);
                }
                Instruction::If { cond, true_branch, false_branch } => {
                    let split_in_true_branch = self.coro_split_mark_in_bb(true_branch);
                    let split_in_false_branch = self.coro_split_mark_in_bb(false_branch);
                    record_bb(true_branch, split_in_true_branch);
                    record_bb(false_branch, split_in_false_branch);
                }
                Instruction::Switch {
                    value: _,
                    default,
                    cases } => {
                    let split_in_default = self.coro_split_mark_in_bb(default);
                    record_bb(default, split_in_default);
                    for SwitchCase { value: _, block } in cases.as_ref().iter() {
                        let split_in_case = self.coro_split_mark_in_bb(block);
                        record_bb(block, split_in_case);
                    }
                }
                _ => {}
            }
        });
        split_in_bb_v
    }
    fn preprocess(&mut self, kernel: &KernelModule) -> FrameBuilder {
        // TODO: where to process captures/args/shared?
        let bb = &kernel.module.entry;
        let mut frame_token_manager = FrameTokenManager::get_instance();
        bb.iter().for_each(|node_ref_present| {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
                Instruction::CoroSplitMark { token } => {
                    unsafe { frame_token_manager.register_frame_token(*token); }
                }
                _ => {}
            }
        });
        FrameBuilder {
            token: frame_token_manager.get_new_token(),
            builder: IrBuilder::new(kernel.pools.clone()),
        }
    }

    fn record_continuation(&mut self, prev: u32, next: u32) {
        let mut continuation = self.continuations.get_mut(&prev).unwrap_or_default();
        continuation.token = prev;
        continuation.next.insert(next);
        let mut continuation = self.continuations.get_mut(&next).unwrap_or_default();
        continuation.token = next;
        continuation.prev.insert(prev);
    }

    fn visit_bb(&mut self, bb: &Pooled<BasicBlock>, mut frame_builder: FrameBuilder,
                node_ref_start: Option<NodeRef>)-> Vec<FrameBuilder> {
        let pools = &frame_builder.builder.pools;

        let mut node_ref_present = match node_ref_start {
            Some(node_ref_start) => node_ref_start,
            None => bb.first.get().next,
        };
        while node_ref_present != bb.last {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
                // coroutine related instructions
                Instruction::CoroSplitMark { token } => unsafe {
                    frame_builder = self.visit_coro_split_mark(*token, frame_builder, &node_ref_present);
                }
                Instruction::CoroSuspend { token } => {
                    frame_builder = self.visit_coro_suspend(*token, frame_builder);
                    return vec![frame_builder]
                }
                Instruction::CoroResume { token } => {
                    unreachable!("Split: CoroResume");
                }

                // 3 Instructions to be processed
                Instruction::Loop { body, cond } => {
                    if self.split_in_bb.contains(&body.as_ptr()) {
                        /// split in body
                        let mut fb_vec = self.visit_bb(body, frame_builder, None);

                        // use a new IrBuilder to get the start part of the body bb (util CoroSuspend)
                        let mut ir_builder = IrBuilder::new(pools.clone());
                        let frame_builder = FrameBuilder {
                            token: INVALID_FRAME_TOKEN,
                            builder: ir_builder,
                        };
                        let mut fb_body_vec = self.visit_bb(body, frame_builder, None);
                        assert_eq!(fb_body_vec.len(), 1);
                        let mut fb_body = fb_body_vec.pop().unwrap();
                        let frame_body = &fb_body.builder.finish();

                        let mut frame_builder_vec = vec![];
                        // replace loop with if
                        for mut fb in fb_vec {
                            // TODO: local/frame may fail in function find_duplicated_node. example: phi cond in loop
                            let dup_cond = self.find_duplicated_node(fb.token, *cond);
                            let true_branch = self.duplicate_block(fb.token, &fb.builder.pools, frame_body);
                            // empty false branch
                            let false_branch = fb.builder.pools.bb_pool.alloc(BasicBlock::new(&pools));
                            fb.builder.if_(dup_cond.clone(), true_branch, false_branch);
                            let fb_next_vec = self.visit_bb(bb, fb, Some(node_ref_present.get().next));
                            frame_builder_vec.extend(fb_next_vec);
                        }
                        return frame_builder_vec
                    } else {
                        self.duplicate_node(frame_builder.token, &mut frame_builder.builder, node_ref_present);
                    }
                }
                Instruction::If { cond, true_branch, false_branch } => {
                    let split_in_true_branch = self.split_in_bb.contains(&true_branch.as_ptr());
                    let split_in_false_branch = self.split_in_bb.contains(&false_branch.as_ptr());
                    if !split_in_true_branch && !split_in_false_branch {
                        self.duplicate_node(frame_builder.token, &mut frame_builder.builder, node_ref_present);
                    }
                    /// split in true/false_branch
                    // TODO
                    if split_in_true_branch {
                        // let mut fb_vec = self.visit_bb(true_branch, frame_builder, None);

                    }
                    if split_in_false_branch {

                    }
                }
                Instruction::Switch { value, cases, default } => {
                    // TODO: split in cases/default
                }

                // other instructions, just deep clone and change the node_ref to new ones
                _ => {
                    self.duplicate_node(frame_builder.token, &mut frame_builder.builder, node_ref_present);
                }
            }
            node_ref_present = node_ref_present.get().next;
        }
        vec![frame_builder]
    }
    fn visit_coro_split_mark(&mut self, token_next: u32, mut frame_builder: FrameBuilder, node_ref: &NodeRef) -> FrameBuilder {
        /// frame_next has not been processed

        let frame_token = frame_builder.token;
        let mut builder = &mut frame_builder.builder;
        let pools = &builder.pools;

        // record continuation
        self.record_continuation(frame_token, token_next);

        // replace CoroSplitMark with CoroSuspend
        let coro_suspend = builder.coro_suspend(token_next);
        node_ref.replace_with(coro_suspend.get());

        // build frame
        let frame_bb = builder.finish();
        let frame = Instruction::CoroFrame {
            token: frame_token,
            body: frame_bb,
        };
        let frame_node = Node::new(CArc::new(frame), CArc::new(Type::Void));
        let frame_node_ref = new_node(&pools, frame_node);
        self.coro_frames.push(frame_node_ref);

        // create a new frame builder for the next frame
        // the next frame must have a CoroResume
        let mut builder = IrBuilder::new(pools.clone());
        builder.coro_resume(token_next);
        FrameBuilder {
            token: token_next,
            builder,
        }
    }
    fn visit_coro_suspend(&mut self, token_next: u32, mut frame_builder: FrameBuilder) -> FrameBuilder {
        /// frame_next is already processed

        let frame_token = frame_builder.token;

        // record continuation
        self.record_continuation(frame_token, token_next);

        // the next frame must have been processed
        // there is no need to create a new frame builder
        frame_builder
    }

    fn split(&mut self, kernel: &KernelModule) {
        // prepare
        let pools = &kernel.pools;
        let mut frame_builder = self.preprocess(kernel);
        let bb = &kernel.module.entry;

        // visit
        frame_builder = self.visit_bb(bb, frame_builder, None).unwrap();
        let mut builder = &mut frame_builder.builder;
        let frame_token = frame_builder.token;
        let frame_bb = builder.finish();
        let frame = Instruction::CoroFrame {
            token: frame_token,
            body: frame_bb,
        };
        let frame_node = Node::new(CArc::new(frame), CArc::new(Type::Void));
        let frame_node_ref = new_node(&pools, frame_node);
        self.coro_frames.push(frame_node_ref);
    }


    // duplicate functions
    fn find_duplicated_block(&self, frame_token: u32, bb: &Pooled<BasicBlock>) -> Pooled<BasicBlock> {
        let ctx = self.module_duplicators.get(&frame_token).unwrap_or_default().current.as_ref().unwrap();
        ctx.blocks.get(&bb.as_ptr()).unwrap().clone()
    }
    fn find_duplicated_node(&self, frame_token: u32, node: NodeRef) -> NodeRef {
        if !node.valid() { return INVALID_REF; }
        let ctx = self.module_duplicators.get(&frame_token).unwrap_or_default().current.as_ref().unwrap();
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
        self.module_duplicators.get_mut(&frame_token).unwrap_or_default()
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
        if let Some(copy) = self.module_duplicators.get(&frame_token).unwrap_or_default()
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
        self.module_duplicators.get_mut(&frame_token).unwrap_or_default().callables.insert(callable.as_ptr(), dup_callable.clone());
        dup_callable
    }
    fn duplicate_block(&mut self, frame_token: u32, pools: &CArc<ModulePools>, bb: &Pooled<BasicBlock>) -> Pooled<BasicBlock> {
        assert!(!self.current.as_ref().unwrap().blocks.contains_key(&bb.as_ptr()),
                "Basic block {:?} has already been duplicated", bb);
        let mut builder = IrBuilder::new(pools.clone());
        bb.iter().for_each(|node| {
            self.duplicate_node(frame_token, &mut builder, node);
        });
        let dup_bb = builder.finish();
        // insert the duplicated block into the map
        self.module_duplicators.get_mut(&frame_token).unwrap_or_default()
            .current.as_mut().unwrap().blocks.insert(bb.as_ptr(), dup_bb.clone());
        dup_bb
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
                    let dup_arg = self.find_duplicated_node(frame_token, *arg);
                    dup_arg
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
                let dup_body = self.duplicate_block(frame_token, &builder.pools, body);
                let dup_cond = self.find_duplicated_node(frame_token, *cond);
                builder.loop_(dup_body, dup_cond)
            }
            Instruction::GenericLoop { prepare, cond, body, update } => {
                let dup_prepare = self.duplicate_block(frame_token, &builder.pools, prepare);
                let dup_body = self.duplicate_block(frame_token, &builder.pools, body);
                let dup_update = self.duplicate_block(frame_token, &builder.pools, update);
                let dup_cond = self.find_duplicated_node(frame_token, *cond);
                builder.generic_loop(dup_prepare, dup_cond, dup_body, dup_update)
            }
            Instruction::Break => builder.break_(),
            Instruction::Continue => builder.continue_(),
            Instruction::If { cond, true_branch, false_branch } => {
                let dup_cond = self.find_duplicated_node(frame_token, *cond);
                let dup_true_branch = self.duplicate_block(frame_token, &builder.pools, true_branch);
                let dup_false_branch = self.duplicate_block(frame_token, &builder.pools, false_branch);
                builder.if_(dup_cond, dup_true_branch, dup_false_branch)
            }
            Instruction::Switch { value, cases, default } => {
                let dup_value = self.find_duplicated_node(frame_token, *value);
                let dup_cases: Vec<_> = cases.iter().map(|case| {
                    let dup_block = self.duplicate_block(frame_token, &builder.pools, &case.block);
                    SwitchCase {
                        value: case.value,
                        block: dup_block,
                    }
                }).collect();
                let dup_default = self.duplicate_block(frame_token, &builder.pools, default);
                builder.switch(dup_value, dup_cases.as_slice(), dup_default)
            }
            Instruction::AdScope { body } => {
                let dup_body = self.duplicate_block(frame_token, &builder.pools, body);
                builder.ad_scope(dup_body)
            }
            Instruction::RayQuery { ray_query, on_triangle_hit, on_procedural_hit } => {
                let dup_ray_query = self.find_duplicated_node(frame_token, *ray_query);
                let dup_on_triangle_hit = self.duplicate_block(frame_token, &builder.pools, on_triangle_hit);
                let dup_on_procedural_hit = self.duplicate_block(frame_token, &builder.pools, on_procedural_hit);
                builder.ray_query(dup_ray_query, dup_on_triangle_hit, dup_on_procedural_hit, node.type_.clone())
            }
            Instruction::AdDetach(body) => {
                let dup_body = self.duplicate_block(frame_token, &builder.pools, body);
                builder.ad_detach(dup_body)
            }
            Instruction::Comment(msg) => builder.comment(msg.clone()),
        };
        // insert the duplicated node into the map
        self.old2new.entry(node_ref).or_default().insert((frame_token, dup_node));
        self.module_duplicators.get_mut(&frame_token).unwrap_or_default()
            .current.as_mut().unwrap().nodes.insert(node_ref, dup_node);
        dup_node
    }
}