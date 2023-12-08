// This file implement def-use analysis for coroutine scopes.
// For each coroutine scope, at each instruction, the analysis will record the set of
// definitions that are alive at that instruction. If the instruction uses some value
// that is not alive at that instruction, then the value must be defined other places
// so we would have to pass the value through the coroutine frame.
// The analysis tries to perform member-wise analysis, so the values that have to be
// passed through the coroutine frame are minimized.

use crate::ir::NodeRef;
use std::collections::{HashMap, HashSet};

pub(crate) struct AccessNodeRef(pub usize);

pub(crate) struct AccessNode {
    node: NodeRef,
    members: HashMap<usize, AccessNodeRef>,
}

pub(crate) struct AccessTree {
    map: HashMap<NodeRef, AccessNodeRef>,
    nodes: Vec<AccessNode>,
}
