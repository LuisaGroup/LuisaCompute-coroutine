// This file implement def-use analysis for coroutine scopes.
// For each coroutine scope, at each instruction, the analysis will record the set of
// definitions that are alive at that instruction. If the instruction uses some value
// that is not alive at that instruction, then the value must be defined other places
// so we would have to pass the value through the coroutine frame.
// The analysis tries to perform member-wise analysis, so the values that have to be
// passed through the coroutine frame are minimized.

use crate::ir::NodeRef;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub(crate) struct AccessNode {
    members: HashMap<usize, Rc<AccessNode>>,
}

#[derive(Debug, Clone)]
pub(crate) struct AccessTree {
    nodes: HashMap<NodeRef, Rc<AccessNode>>,
}

impl AccessTree {
    fn insert(&mut self, node: NodeRef, access_chain: &[usize]) {
        todo!()
    }

    fn all(&self, node: NodeRef, access_chain: &[usize]) -> bool {
        todo!()
    }

    fn any(&self, node: NodeRef, access_chain: &[usize]) -> bool {
        todo!()
    }

    fn intersect(&self, other: &Self) -> Self {
        todo!()
    }
}

struct CoroScopeDefUse {
    external_uses: AccessTree,
    internal_defs: HashMap<u32, AccessTree>, // suspend -> defs
}

struct CoroDefUseAnalysis {
    scopes: HashMap<NodeRef, AccessTree>,
}
