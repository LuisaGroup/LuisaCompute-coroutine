use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use crate::display::DisplayIR;
use crate::ir::{NodeRef};

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
