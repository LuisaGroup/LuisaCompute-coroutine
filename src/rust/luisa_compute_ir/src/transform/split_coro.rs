use crate::analysis::coro_frame_v4::CoroFrameAnalyser;
// use crate::analysis::coro_graph::CoroPreliminaryGraph;
use crate::analysis::coro_graph::CoroGraph;
use crate::ir::{CallableModule, Module};
use crate::transform::Transform;

pub struct SplitCoro;

impl Transform for SplitCoro {
    fn transform_callable(&self, callable: CallableModule) -> CallableModule {
        println!("SplitCoro::transform_module");
        let graph = CoroGraph::from(&callable.module);
        CoroFrameAnalyser::analyse(&graph, &callable);
        todo!()
    }
}
