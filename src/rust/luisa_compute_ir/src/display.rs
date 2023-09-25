use crate::ir::{BasicBlock, CallableModule, KernelModule};
use crate::{
    context::is_type_equal,
    ir,
    ir::{Instruction, Module, NodeRef, SwitchCase, Type},
    Pooled,
};
use std::collections::{HashMap, HashSet};

pub struct DisplayIR {
    output: String,
    map: HashMap<usize, usize>,
    cnt: usize,
}

impl DisplayIR {
    pub fn new() -> Self {
        Self {
            output: String::new(),
            map: HashMap::new(),
            cnt: 0,
        }
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.cnt = 0;
    }

    pub fn display_ir(&mut self, module: &Module) -> String {
        self.clear();
        self.display_ir_bb(&module.entry, 0, false)
    }

    pub fn display_ir_kernel(&mut self, kernel: &KernelModule) -> String {
        self.clear();
        self.output += &format!("\n{:-^40}\n", "Args");
        for arg in kernel.args.as_ref() {
            self.display(*arg, 0, false);
        }
        self.output += &format!("\n{:-^40}\n", "Shared");
        for arg in kernel.shared.as_ref() {
            self.display(*arg, 0, false);
        }
        self.output += &format!("\n{:-^40}\n", "Captures");
        for arg in kernel.captures.as_ref() {
            self.display(arg.node, 0, false);
        }
        self.output += &format!("\n{:-^40}\n", "Module");
        self.display_ir_bb(&kernel.module.entry, 0, false)
    }

    pub fn display_ir_callable(&mut self, callable: &CallableModule) -> String {
        self.clear();
        self.output += &format!("\n{:-^40}\n", "Args");
        for arg in callable.args.as_ref() {
            self.display(*arg, 0, false);
        }
        self.output += &format!("\n{:-^40}\n", "Captures");
        for arg in callable.captures.as_ref() {
            self.display(arg.node, 0, false);
        }
        self.output += &format!("\n{:-^40}\n", "Module");
        self.display_ir_bb(&callable.module.entry, 0, false)
    }

    pub fn display_ir_bb(
        &mut self,
        bb: &Pooled<BasicBlock>,
        ident: usize,
        no_new_line: bool,
    ) -> String {
        for node in bb.nodes().iter() {
            self.display(*node, ident, no_new_line);
        }
        self.output.clone()
    }

    pub fn display_existent_nodes(&self, nodes: &HashSet<NodeRef>) -> String {
        let mut node_cnts: Vec<_> = nodes.iter().map(|node| self.get(node)).collect();
        node_cnts.sort_unstable();
        let mut node_cnts: Vec<_> = node_cnts.iter().map(|node| format!("${}", node)).collect();
        format!("{{{}}}", node_cnts.join(", "))
    }

    fn get_or_insert(&mut self, node: &NodeRef) -> usize {
        if node.valid() {
            self.map
                .get(&node.0)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| {
                    self.map.insert(node.0, self.cnt);
                    self.cnt += 1;
                    self.cnt - 1
                })
        } else {
            usize::MAX
        }
    }

    fn get(&self, node: &NodeRef) -> usize {
        self.map
            .get(&node.0)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| panic!("{:?} not found in map", node))
    }

    fn add_ident(&mut self, ident: usize) {
        for _ in 0..ident {
            self.output += "    ";
        }
    }

    fn display(&mut self, node: NodeRef, ident: usize, no_new_line: bool) {
        let instruction = &node.get().instruction;
        let type_ = &node.get().type_;
        self.add_ident(ident);
        match instruction.as_ref() {
            Instruction::Buffer => {
                let temp = format!("${}: Buffer<{}> [param]", self.get_or_insert(&node), type_,);
                self.output += temp.as_str();
            }
            Instruction::Bindless => {
                let temp = format!(
                    "${}: Bindless<{}> [param]",
                    self.get_or_insert(&node),
                    type_,
                );
                self.output += temp.as_str();
            }
            Instruction::Texture2D => {
                let temp = format!(
                    "${}: Texture2D<{}> [param]",
                    self.get_or_insert(&node),
                    type_,
                );
                self.output += temp.as_str();
            }
            Instruction::Texture3D => {
                let temp = format!(
                    "${}: Texture3D<{}> [param]",
                    self.get_or_insert(&node),
                    type_,
                );
                self.output += temp.as_str();
            }
            Instruction::Accel => {
                let temp = format!("${}: Accel<{}> [param]", self.get_or_insert(&node), type_,);
                self.output += temp.as_str();
            }
            Instruction::Shared => {
                let temp = format!("${}: Shared<{}> [param]", self.get_or_insert(&node), type_,);
                self.output += temp.as_str();
            }
            Instruction::Uniform => {
                let temp = format!("${}: {} [param]", self.get_or_insert(&node), type_,);
                self.output += temp.as_str();
            }
            Instruction::Local { init } => {
                let temp = format!(
                    "${}: {} = ${} [init]",
                    self.get_or_insert(&node),
                    type_,
                    self.get_or_insert(init)
                );
                self.output += temp.as_str();
            }
            Instruction::Argument { by_value } => {
                let temp = format!("${}", self.get_or_insert(&node));
                let by_value_prefix = if *by_value { "" } else { "&" };
                self.output += by_value_prefix;
                self.output += temp.as_str();
            }
            Instruction::UserData(_) => self.output += "Userdata",
            Instruction::Invalid => self.output += "INVALID",
            Instruction::Const(c) => {
                let temp = format!("${}: {} = Const {}", self.get_or_insert(&node), type_, c,);
                self.output += temp.as_str();
            }
            Instruction::Update { var, value } => {
                let temp = format!(
                    "${} = ${}",
                    self.get_or_insert(var),
                    self.get_or_insert(value),
                );
                self.output += temp.as_str();
            }
            Instruction::Call(func, args) => {
                if !is_type_equal(type_, &Type::void()) {
                    let tmp = format!("${}: {} = ", self.get_or_insert(&node), type_,);
                    self.output += tmp.as_str();
                }

                let args = args
                    .as_ref()
                    .iter()
                    .map(|arg| format!("${}", self.get_or_insert(arg)))
                    .collect::<Vec<_>>()
                    .join(", ");

                self.output += format!("Call {:?}({})", func, args,).as_str();
            }
            Instruction::Phi(_) => {
                let n = self.get_or_insert(&node);
                self.output += &format!("${}: Phi", n)
            }
            Instruction::Break => self.output += "break",
            Instruction::Continue => self.output += "continue",
            Instruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                let temp = format!("if ${} {{\n", self.get_or_insert(cond));
                self.output += temp.as_str();
                for node in true_branch.nodes().iter() {
                    self.display(*node, ident + 1, false);
                }
                if !false_branch.nodes().is_empty() {
                    self.add_ident(ident);
                    self.output += "} else {\n";
                    for node in false_branch.nodes().iter() {
                        self.display(*node, ident + 1, false);
                    }
                }
                self.add_ident(ident);
                self.output += "}";
            }
            Instruction::Switch {
                value,
                default,
                cases,
            } => {
                let temp = format!("switch ${} {{\n", self.get_or_insert(value));
                self.output += temp.as_str();
                for SwitchCase { value, block } in cases.as_ref() {
                    self.add_ident(ident + 1);
                    let temp = format!("{} => {{\n", value);
                    self.output += temp.as_str();
                    for node in block.nodes().iter() {
                        self.display(*node, ident + 2, false);
                    }
                    self.add_ident(ident + 1);
                    self.output += "}\n";
                }
                self.add_ident(ident + 1);
                self.output += "default => {\n";
                for node in default.nodes().iter() {
                    self.display(*node, ident + 2, false);
                }
                self.add_ident(ident + 1);
                self.output += "}\n";
                self.output += "}";
            }
            Instruction::RayQuery { .. } => todo!(),
            Instruction::Loop { body, cond } => {
                let temp = format!("Loop {{\n");
                self.output += temp.as_str();
                for node in body.nodes().iter() {
                    self.display(*node, ident + 1, false);
                }
                self.add_ident(ident);
                let temp = format!("}} (${})", self.get_or_insert(cond));
                self.output += &temp;
            }
            Instruction::GenericLoop {
                prepare,
                cond,
                body,
                update,
            } => {
                self.output += "for ";
                for (index, node) in prepare.nodes().iter().enumerate() {
                    self.display(*node, 0, true);
                    if index != prepare.nodes().len() - 1 {
                        self.output += ", ";
                    }
                }
                self.output += " | ";
                self.display(*cond, 0, true);
                self.output += " | ";
                for (index, node) in update.nodes().iter().enumerate() {
                    self.display(*node, 0, true);
                    if index != update.nodes().len() - 1 {
                        self.output += ", ";
                    }
                }
                self.output += " {\n";
                for node in body.nodes().iter() {
                    self.display(*node, ident + 1, false);
                }
                self.add_ident(ident);
                self.output += "}";
            }
            Instruction::AdScope { body, forward, .. } => {
                self.output += &format!(
                    "{}AdScope {{\n",
                    if *forward { "Forward" } else { "Reverse" }
                );
                for node in body.nodes().iter() {
                    self.display(*node, ident + 1, false);
                }
                self.add_ident(ident);
                self.output += "}";
            }
            Instruction::AdDetach(bb) => {
                self.output += "AdDetach {\n";
                for node in bb.nodes().iter() {
                    self.display(*node, ident + 1, false);
                }
                self.add_ident(ident);
                self.output += "}";
            }
            Instruction::Comment(_) => {}
            Instruction::Return(_) => {
                self.output += "return\n";
            }
            Instruction::CoroSplitMark { token } => {
                self.output += format!("CoroSplitMark({})", token).as_str();
            }
            Instruction::CoroSuspend { token } => {
                self.output += format!("CoroSuspend({})", token).as_str();
            }
            Instruction::CoroRegister { token, value, var } => {
                let temp = format!(
                    "CoroRegister(at {}, ${}, to {})\n",
                    token,
                    self.get(value),
                    var
                );
                self.output += temp.as_str();
            }
            Instruction::CoroResume { token } => {
                self.output += format!("CoroResume({})", token).as_str();
            }
            Instruction::Print { .. } => {}
        }
        if !no_new_line {
            self.output += "\n";
        }
    }
}
