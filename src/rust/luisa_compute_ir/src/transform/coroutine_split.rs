use std::ops::Deref;
use super::Transform;

use crate::{*, display::DisplayIR};
use ir::*;
use crate::analysis::coro_frame::CoroFrameAnalyser;
use crate::transform::split::SplitManager;

pub struct Coroutine;

struct CoroutineImpl {}

impl CoroutineImpl {
    fn split_coroutine(callable: CallableModule) -> CallableModule {
        println!("{:-^40}", " Before split ");
        let mut display_ir = DisplayIR::new();
        let result = display_ir.display_ir_callable(&callable);
        println!("{}", result);

        let mut coro_frame_analyser = CoroFrameAnalyser::new();
        coro_frame_analyser.analyse_callable(&callable);
        println!("{}", coro_frame_analyser.display_active_vars(&display_ir));
        println!("{}", coro_frame_analyser.display_continuations());

        let coroutine_entry = SplitManager::split(coro_frame_analyser, &callable);
        println!("{:-^40}", " After split ");
        for (token, coro) in coroutine_entry.subroutine_ids.iter().zip(coroutine_entry.subroutines.iter()) {
            let result = DisplayIR::new().display_ir_callable(coro.as_ref());
            println!("{:-^40}\n{}", format!(" CoroScope {} ", token), result);
        }

        coroutine_entry
        // unimplemented!("Coroutine split");
    }
}

impl Transform for Coroutine {
    fn transform_callable(&self, callable: CallableModule) -> CallableModule {
        CoroutineImpl::split_coroutine(callable)
    }
}
