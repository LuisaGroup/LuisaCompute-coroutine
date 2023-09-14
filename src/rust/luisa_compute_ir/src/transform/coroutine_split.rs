use super::Transform;

use crate::{*, display::DisplayIR};
use ir::*;
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
        println!("\n--------------- Before split --------------\n");
        let result = DisplayIR::new().display_ir_callable(&callable);
        println!("{}", result);

        let mut sm = SplitManager::new();
        sm.split(&callable);
        println!("\n--------------- After split ---------------\n");
        for (token, sb) in sm.coro_scopes.iter() {
            let result = DisplayIR::new().display_ir_bb(sb, 0, false);
            println!("CoroScope {}:\n{}", token, result);
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
