use std::collections::HashMap;
use crate::ir::{BasicBlock, Module, NodeRef};
use crate::{Pool, Pooled};
use super::Transform;


struct BBTreeNode {
    node: Pooled<BasicBlock>,
    children: Vec<Pooled<BBTreeNode>>,
}

impl BBTreeNode {
    fn new(node: Pooled<BasicBlock>) -> Self {
        Self {
            node,
            children: Vec::new(),
        }
    }
}

struct BBTree {
    pools: Pool<BBTreeNode>,
    root: Pooled<BBTreeNode>,
    map: HashMap<Pooled<BasicBlock>, Pooled<BBTreeNode>>,
}

impl BBTree {
    fn new(root: Pooled<BasicBlock>) -> Self {
        let pools = Pool::new();
        let mut tree = Self {
            pools,
            root: Pooled::null(),
            map: HashMap::new(),
        };
        tree.new_node(root);
        tree
    }

    fn new_node(&mut self, node: Pooled<BasicBlock>) -> Pooled<BBTreeNode> {
        let node = self.pools.alloc(BBTreeNode::new(node));
        // self.map.insert(node.node, node);    // todo
        node
    }
}

struct DemotionImpl {
    module: Module,
}

impl DemotionImpl {
    fn demotion(module: Module) -> Module {
        let mut impl_ = Self {
            module,
        };
        todo!()
    }
}

pub struct Demotion;

impl Transform for Demotion {
    fn transform_module(&self, module: Module) -> Module {
        DemotionImpl::demotion(module)
    }
}