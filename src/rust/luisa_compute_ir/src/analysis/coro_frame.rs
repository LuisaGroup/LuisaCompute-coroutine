use std::collections::{HashMap, HashSet};

use crate::ir::{NodeRef, Usage, UsageMark};

pub struct FrameManager {
    pub mark: HashMap<NodeRef, Usage>,
    pub frame_in: HashSet<NodeRef>,
}

impl FrameManager {
    fn new() -> Self {
        Self {
            mark: HashMap::new(),
            frame_in: HashSet::new(),
        }
    }

    fn mark(&mut self, node_ref: NodeRef, flag: UsageMark) {
        // split mod may generate uninitialized resource usage
        self.mark.get_mut(&node_ref).map(|item| {
            *item = match item {
                None => Usage::NONE.mark(flag),
                _ => item.mark(flag),
            };
        });
    }
}