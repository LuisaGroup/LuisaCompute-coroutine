// use crate::analysis::coro_graph::CoroPreliminaryGraph;
use crate::analysis::coro_graph::CoroGraph;
use crate::ir::Module;
use crate::transform::Transform;

pub struct SplitCoro;

impl Transform for SplitCoro {
    fn transform_module(&self, module: Module) -> Module {
        println!("SplitCoro::transform_module");
        let graph = CoroGraph::from(&module);
        graph.dump();
        module
    }
}
