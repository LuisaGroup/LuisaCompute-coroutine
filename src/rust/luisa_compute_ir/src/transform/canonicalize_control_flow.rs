/*
 * This file implements the control flow canonicalization transform.
 * This transform removes all break/continue/early-return statements, in the following steps:
 * 1. Lower generic loops to do-while loops.
 * 2.
 */

use crate::ir::Module;
use crate::transform::Transform;

pub struct CanonicalizeControlFlow;

/*
 * This transform lowers generic loops to do-while loops as the following template shows:
 * - Original
 *   generic_loop {
 *       prepare;// typically the computation of the loop condition
 *       if cond {
 *           body;
 *           update; // continue goes here
 *       }
 *   }
 * - Transformed
 *   do {
 *       loop_break = false;
 *       prepare();
 *       if (!cond()) break;
 *       loop {
 *           body {
 *               // break => { loop_break = true; break; }
 *               // continue => { break; }
 *           }
 *           break;
 *       }
 *       if (loop_break) break;
 *       update();
 *   } while (true)
 */
struct LowerGenericLoops;

impl LowerGenericLoops {

}

impl Transform for CanonicalizeControlFlow {
    fn transform_module(&self, module: Module) -> Module {
        todo!()
    }
}
