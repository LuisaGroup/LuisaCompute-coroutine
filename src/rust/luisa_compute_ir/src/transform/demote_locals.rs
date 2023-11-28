// This file implements the local variable demotion transform.
//
// This transform demotes local variables to its lowest possible scope, i.e., to right
// before the lowest common ancestor of all its references, in the following steps:
// 1. Construct a scope tree from the AST of the module, where each node is a instruction
//    that contains nested basic blocks.
// 2. Recursively visit each node in the scope tree and propagate the referenced variables
//    to its parent node.
// 3. At each node for each propagated variable, demote the variable definition here if
//    following conditions are met:
//    a) the variable has not been demoted by the node's ancestor, and
//    b) the variable is found propagated to more than one child node.
// 4. Move variable definitions to proper places and remove unreferenced variables if any.
//
// Note: the `reg2mem` transform should be applied before this transform.

use crate::analysis::scope_tree::ScopeTree;

pub struct DemoteLocals;
