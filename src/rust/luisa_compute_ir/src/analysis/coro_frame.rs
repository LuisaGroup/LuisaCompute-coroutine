// This file implements the coroutine frame analysis, which uses the result of the
// coroutine definition-use analysis to determine the layout of the coroutine frame,
// i.e. the memory layout of the state structure that is passed around between the
// subroutines of the coroutine.
//
// Current implementation handles the task in the following steps:
// 1. Walk the original module to obtain the order of all the IR nodes.
// 2. Use the `external_uses` from the coroutine definition-use analysis to determine
//    the set of coroutine arguments that *must* be passed through the coroutine frame.
//    At present, we simply take the union of the external uses of all the subroutines.
//    In the future, we might use, e.g., liveness analysis and register allocation to
//    reduce the number of arguments that need to be passed through the frame. We also
//    support user-specified `designated` arguments and simply add them to the frame.
//    - Note: we cannot simply coalesce them with the arguments already in the frame,
//      as lifetimes of the designated arguments are not controlled by the coroutine.
// 3. We take the active members from of each arguments in the frame and sort them by
//    their size and order in the IR to help maintain a stable frame layout across runs.
//    Packing is applied to squeeze the gaps between the aggregate fields.
// 4.
