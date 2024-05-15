use crate::analysis::utility::ChainedUnionSet;
use crate::ir::{
    collect_nodes_with_block, BasicBlock, CallableModule, Instruction, Module, ModulePools,
    NodeRef, PhiIncoming,
};
use crate::transform::Transform;
use crate::{CArc, CBoxedSlice, Pooled};
use std::collections::HashMap;

struct CopyPropagationImpl {
    pools: CArc<ModulePools>,
    module_original: Pooled<BasicBlock>,

    use_root: ChainedUnionSet<NodeRef>,
}

impl CopyPropagationImpl {
    fn new(module: &Module) -> Self {
        Self {
            pools: module.pools.clone(),
            module_original: module.entry,
            use_root: ChainedUnionSet::new(),
        }
    }
    fn copy_propagate(&mut self) {
        // record
        let (nodes, node2block) = collect_nodes_with_block(self.module_original);
        for node in nodes.iter() {
            let instruction = node.get().instruction.as_ref();
            match instruction {
                Instruction::Phi(incomings) => {
                    if incomings.len() == 1 {
                        self.use_root.set_root(*node, incomings[0].value);
                    }
                }
                _ => {}
            }
        }

        // redirect use to its root
        for node in nodes {
            self.redirect_use(node, &node2block);
        }
    }

    fn redirect_use(&mut self, node: NodeRef, node2block: &HashMap<NodeRef, Pooled<BasicBlock>>) {
        let instruction = node.get().instruction.as_ref();
        match instruction {
            Instruction::Invalid => {
                unreachable!("Invalid node should not appear in non-sentinel nodes");
            }
            Instruction::Buffer
            | Instruction::Bindless
            | Instruction::Texture2D
            | Instruction::Texture3D
            | Instruction::Accel
            | Instruction::Shared
            | Instruction::Uniform
            | Instruction::Argument { .. } => {
                unreachable!("{:?} should not appear in basic block", instruction)
            }

            Instruction::Const(_) | Instruction::Comment(_) | Instruction::UserData(_) => {}

            Instruction::Local { init } => {
                let init_new = self.use_root.root(init);
                if init_new != *init {
                    node.get_mut().instruction = CArc::new(Instruction::Local { init: init_new });
                }
            }
            Instruction::Update { var, value } => {
                let value_new = self.use_root.root(value);
                if value_new != *value {
                    node.get_mut().instruction = CArc::new(Instruction::Update {
                        var: *var,
                        value: value_new,
                    });
                }
            }
            Instruction::Call(func, args) => {
                let mut changed = false;
                let mut args_new = vec![];
                for arg in args.iter() {
                    let arg_new = self.use_root.root(arg);
                    args_new.push(arg_new);
                    if arg_new != *arg {
                        changed = true;
                    }
                }
                if changed {
                    let args = CBoxedSlice::new(args_new);
                    node.get_mut().instruction =
                        CArc::new(Instruction::Call(func.clone(), args.clone()));
                }
            }
            Instruction::Phi(incomings) => {
                if incomings.len() == 1 {
                    // copy propagation, remove this phi node
                    node.remove();
                } else {
                    // set incomings to their roots
                    let mut changed = false;
                    let mut incomings_new = vec![];
                    for incoming in incomings.iter() {
                        let incoming_value_root = self.use_root.root(&incoming.value);
                        if incoming_value_root == incoming.value {
                            incomings_new.push(incoming.clone());
                            continue;
                        }

                        changed = true;
                        let block = if let Some(block) = node2block.get(&incoming_value_root) {
                            *block
                        } else {
                            // phi incoming is callable arg
                            self.module_original
                        };
                        let incoming_new = PhiIncoming {
                            value: incoming_value_root,
                            block,
                        };
                        incomings_new.push(incoming_new);
                    }

                    let incomings_new = CBoxedSlice::new(incomings_new);
                    if changed {
                        node.get_mut().instruction = CArc::new(Instruction::Phi(incomings_new));
                    }
                }
            }
            Instruction::Return(value) => {
                if value.valid() {
                    let value_new = self.use_root.root(value);
                    if value_new != *value {
                        node.get_mut().instruction = CArc::new(Instruction::Return(value_new));
                    }
                }
            }
            Instruction::CoroRegister { value, name } => {
                let value_new = self.use_root.root(value);

                if value_new != *value {
                    node.get_mut().instruction = CArc::new(Instruction::CoroRegister {
                        value: value_new,
                        name: name.clone(),
                    })
                }
            }
            Instruction::Print { fmt, args } => {
                let mut changed = false;
                let mut args_new = Vec::new();
                for arg in args.as_ref() {
                    let arg_new = self.use_root.root(arg);
                    args_new.push(arg_new);
                    if arg_new != *arg {
                        changed = true;
                    }
                }

                if changed {
                    let args_new = CBoxedSlice::new(args_new);
                    node.get_mut().instruction = CArc::new(Instruction::Print {
                        fmt: fmt.clone(),
                        args: args_new,
                    });
                }
            }

            Instruction::Loop { cond, body } => {
                let cond_new = self.use_root.root(cond);
                if cond_new != *cond {
                    node.get_mut().instruction = CArc::new(Instruction::Loop {
                        cond: cond_new,
                        body: *body,
                    })
                }
            }
            Instruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                let cond_new = self.use_root.root(cond);
                if cond_new != *cond {
                    node.get_mut().instruction = CArc::new(Instruction::If {
                        cond: cond_new,
                        true_branch: *true_branch,
                        false_branch: *false_branch,
                    })
                }
            }
            Instruction::Switch {
                value,
                default,
                cases,
            } => {
                let value_new = self.use_root.root(value);
                if value_new != *value {
                    node.get_mut().instruction = CArc::new(Instruction::Switch {
                        value: value_new,
                        default: *default,
                        cases: cases.clone(),
                    })
                }
            }

            Instruction::GenericLoop { .. } | Instruction::Break | Instruction::Continue => {
                unreachable!("{:?} should be lowered in CCF", instruction);
            }

            Instruction::AdScope { .. } | Instruction::AdDetach(_) => {
                unimplemented!("{:?} unimplemented in Mem2Reg", instruction);
            }

            Instruction::RayQuery { .. } => {
                todo!();
            }

            Instruction::CoroSplitMark { .. } => {}
            Instruction::CoroSuspend { .. } | Instruction::CoroResume { .. } => {
                unreachable!("{:?} unreachable in Mem2Reg", instruction);
            }
        }
    }
}

pub struct CopyPropagation;

impl Transform for CopyPropagation {
    fn transform_module(&self, module: Module) -> Module {
        let mut impl_ = CopyPropagationImpl::new(&module);
        impl_.copy_propagate();
        module
    }
}
