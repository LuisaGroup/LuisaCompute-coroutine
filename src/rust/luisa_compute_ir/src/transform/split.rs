use crate::{CArc, Pooled};
use crate::ir::{SwitchCase, Module, Instruction, BasicBlock, KernelModule, duplicate_kernel, IrBuilder, ModulePools, NodeRef};

pub struct SplitManager {
}

impl SplitManager {
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
    |     resume(delta); |     |
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

    fn split_visit_node(&mut self, node_ref: NodeRef) -> Vec<Instruction::CoroFrame> {

    }
    fn split(&mut self, kernel: &KernelModule) -> Vec<Instruction::CoroFrame> {
        let mut coro_frames: Vec<Instruction::CoroFrame> = vec![];
        let mut builder = IrBuilder::new(CArc::new(ModulePools::new()));

        let mut node_ref_present = kernel.module.entry.first.get().next;
        while node_ref_present != kernel.module.entry.last {
            let node = node_ref_present.get();
            match node.instruction.as_ref() {
                Instruction::CoroSplitMark { token } => unsafe {
                    builder.coro_suspend(*token);

                    let bb = builder.finish();
                    let frame = Instruction::CoroFrame {
                        token: *token,
                        body: bb,
                    };
                    coro_frames.push(frame);

                    builder = IrBuilder::new(CArc::new(ModulePools::new()));
                    builder.coro_resume(*token);
                }
                Instruction::CoroSuspend { token } => {

                }
                Instruction::Loop { body, cond } => {

                }
                Instruction::If { cond, true_branch, false_branch } => {

                }
                Instruction::Switch { value, cases, default } => {

                }
                _ => {
                    // other instructions, just copy
                    builder.append(node.clone());
                }
            }
            node_ref_present = node_ref_present.get().next;
        }
        coro_frames
    }
}