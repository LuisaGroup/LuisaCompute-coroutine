use super::Transform;

use crate::{*, display::DisplayIR};
use ir::*;
use crate::analysis::coro_frame::CoroFrameAnalyser;
use crate::transform::split::SplitManager;

pub struct Coroutine;

struct CoroutineImpl {}

impl CoroutineImpl {
    fn new(model: &Module, state: NodeRef) -> Self {
        Self {}
    }
}

impl Transform for Coroutine {
    fn transform_callable(&self, callable: CallableModule) -> CallableModule {
        println!("{:-^40}", "Before split");
        let mut display_ir = DisplayIR::new();
        let result = display_ir.display_ir_callable(&callable);
        println!("{}", result);

        let mut coro_frame_analyser = CoroFrameAnalyser::new();
        coro_frame_analyser.analyse_callable(&callable);
        println!("{}", coro_frame_analyser.display_active_vars(&display_ir));

        let mut sm = SplitManager::split(coro_frame_analyser, &callable);
        println!("{:-^40}", "After split");
        for (token, sb) in sm.coro_scopes.iter() {
            let result = DisplayIR::new().display_ir_bb(sb, 0, false);
            println!("{:-^40}\n{}", format!("CoroScope {}:", token), result);
        }

        unimplemented!("Coroutine split");

        // let mut entry = module.entry;
        // // *entry.get_mut() = todo!();
        // let ret=Module {
        //     kind: module.kind,
        //     entry,
        //     pools: module.pools,
        // };
        // callable.args[0].get_mut().type_=imp.corostate_type;
        // CallableModule {
        //     module:ret,
        //     args:callable.args,
        //     subroutine_ids:CBoxedSlice::new(vec![]),
        //     subroutines:CBoxedSlice::new(vec![]),
        //     ..callable
        // }
    }
}
