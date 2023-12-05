use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use crate::display::DisplayIR;
use crate::ir::{Func, Instruction, NodeRef};

// check if the node is uniform (can be move to anywhere in the module before it is used)
pub(crate) fn is_uniform_value(node: &NodeRef) -> bool {
    match node.get().instruction.as_ref() {
        Instruction::Uniform => true,
        Instruction::Argument { by_value } => *by_value,
        Instruction::Const(_) => true,
        Instruction::Call(func, _) => match func {
            Func::ZeroInitializer
            | Func::DispatchId
            | Func::ThreadId
            | Func::BlockId
            | Func::WarpLaneId
            | Func::DispatchSize
            | Func::WarpSize
            | Func::CoroId => true,
            _ => false,
        },
        _ => false,
    }
}

// check if the node's value source is copiable
pub(crate) fn value_copiable(node: &NodeRef) -> bool {
    match node.get().instruction.as_ref() {
        Instruction::Uniform => true,
        Instruction::Argument { by_value } => *by_value,
        Instruction::Const(_) => true,
        Instruction::Call(func, _) => match func {
            Func::ZeroInitializer
            | Func::DispatchId
            | Func::ThreadId
            | Func::BlockId
            | Func::WarpLaneId
            | Func::DispatchSize
            | Func::WarpSize
            | Func::CoroId
            | Func::CoroToken => true,
            _ => false,
        },
        _ => false,
    }
}


// Singleton pattern for DisplayIR
pub(crate) struct LazyDisplayIR {
    inner: Option<DisplayIR>,
}

impl LazyDisplayIR {
    const fn new() -> Self {
        Self { inner: None }
    }
}

impl LazyDisplayIR {
    pub(crate) fn get(&mut self) -> &mut DisplayIR {
        if self.inner.is_none() {
            self.inner = Some(DisplayIR::new());
        }
        self.inner.as_mut().unwrap()
    }
}

pub(crate) static mut DISPLAY_IR_DEBUG: LazyDisplayIR = LazyDisplayIR::new();  // for DEBUG

pub(crate) fn display_node_map2set(target: &HashMap<NodeRef, HashSet<i32>>) -> String {
    let output: Vec<_> = target.iter().map(|(node_ref, number)| unsafe {
        let node = DISPLAY_IR_DEBUG.get().var_str(node_ref);
        let number = BTreeSet::from_iter(number.iter().cloned());
        let number = format!("{:?}", number);
        let output = format!("{}: {}", node, number);
        (node, output)
    }).collect();
    let output = BTreeMap::from_iter(output);
    let output: Vec<_> = output.iter().map(|(_, output)| output.as_str()).collect();
    let output = output.join(", ");
    let output = format!("{{{}}}", output);
    output
}

pub(crate) fn display_node_map(target: &HashMap<NodeRef, i32>) -> String {
    let output: Vec<_> = target.iter().map(|(node_ref, number)| unsafe {
        let node = DISPLAY_IR_DEBUG.get().var_str(node_ref);
        let output = format!("{}: {}", node, number);
        (node, output)
    }).collect();
    let output = BTreeMap::from_iter(output);
    let output: Vec<_> = output.iter().map(|(_, output)| output.as_str()).collect();
    let output = output.join(", ");
    let output = format!("{{{}}}", output);
    output
}

pub(crate) fn display_node_set(target: &HashSet<NodeRef>) -> String {
    unsafe { DISPLAY_IR_DEBUG.get().vars_str(target) }
}