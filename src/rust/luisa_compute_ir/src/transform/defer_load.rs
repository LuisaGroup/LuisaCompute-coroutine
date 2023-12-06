// This file implements the variable load deferring pass.
//
// This transform pass extends the GEP chain and defers the load of variables to
// the latest possible point, so we can reduce the number of visited members and
// simplify the following usage analysis.
// The pass is done in the following steps:
// 1. Glob all `Load` instructions for aggregates and find all `Extract` nodes on
//    the loaded value with *uniform* indices.
// 2. For each found `Extract` node, replace it with a `Load` node with a GEP chain
//    and move the it to right behind the original `Load` instruction.
// 3. Re-scan the module and remove all `Load` instructions whose value is not used.

use crate::analysis::utility::is_uniform_value;
use crate::ir::{BasicBlock, Func, Instruction, IrBuilder, Module, NodeRef};
use crate::transform::Transform;
use std::collections::{HashMap, HashSet};

pub struct DeferLoad;

struct DeferLoadImpl;

struct AggregateLoadExtract {
    map: HashMap<NodeRef, Vec<NodeRef>>,
    indices: HashMap<NodeRef, Vec<NodeRef>>,
}

impl DeferLoadImpl {
    fn get_extract_root_aggregate(node: NodeRef) -> Option<NodeRef> {
        if let Instruction::Call(func, args) = node.get().instruction.as_ref() {
            match func {
                Func::Load => {
                    if args[0].type_().is_aggregate() {
                        Some(node)
                    } else {
                        None
                    }
                }
                Func::ExtractElement => {
                    if args.iter().skip(1).all(|arg| is_uniform_value(arg)) {
                        Self::get_extract_root_aggregate(args[0])
                    } else {
                        None
                    }
                }
                _ => None,
            }
        } else {
            None
        }
    }

    fn construct_gep_chain_indices(node: NodeRef, indices: &mut Vec<NodeRef>) {
        match node.get().instruction.as_ref() {
            Instruction::Call(func, args) => match func {
                Func::ExtractElement => {
                    Self::construct_gep_chain_indices(args[0], indices);
                    indices.extend(args.iter().skip(1));
                }
                Func::Load => {}
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    fn include_if_aggregate_load_extract(node: NodeRef, result: &mut AggregateLoadExtract) {
        if let Some(root) = Self::get_extract_root_aggregate(node) {
            result.map.entry(root).or_insert_with(Vec::new).push(node);
            let mut indices = Vec::new();
            Self::construct_gep_chain_indices(node, &mut indices);
            result.indices.insert(node, indices);
        }
    }

    fn glob_aggregate_load_extract_in_block(bb: &BasicBlock, result: &mut AggregateLoadExtract) {
        for node in bb.iter() {
            match node.get().instruction.as_ref() {
                Instruction::Call(func, args) => match func {
                    Func::ExtractElement => Self::include_if_aggregate_load_extract(node, result),
                    _ => {}
                },
                Instruction::Loop { body, .. } => {
                    Self::glob_aggregate_load_extract_in_block(body, result);
                }
                Instruction::GenericLoop {
                    prepare,
                    body,
                    update,
                    ..
                } => {
                    Self::glob_aggregate_load_extract_in_block(prepare, result);
                    Self::glob_aggregate_load_extract_in_block(body, result);
                    Self::glob_aggregate_load_extract_in_block(update, result);
                }
                Instruction::If {
                    true_branch,
                    false_branch,
                    ..
                } => {
                    Self::glob_aggregate_load_extract_in_block(true_branch, result);
                    Self::glob_aggregate_load_extract_in_block(false_branch, result);
                }
                Instruction::Switch { cases, default, .. } => {
                    for case in cases.iter() {
                        Self::glob_aggregate_load_extract_in_block(&case.block, result);
                    }
                    Self::glob_aggregate_load_extract_in_block(default, result);
                }
                Instruction::AdScope { body, .. } => {
                    Self::glob_aggregate_load_extract_in_block(body, result);
                }
                Instruction::RayQuery {
                    on_triangle_hit,
                    on_procedural_hit,
                    ..
                } => {
                    Self::glob_aggregate_load_extract_in_block(on_triangle_hit, result);
                    Self::glob_aggregate_load_extract_in_block(on_procedural_hit, result);
                }
                Instruction::AdDetach(body) => {
                    Self::glob_aggregate_load_extract_in_block(body, result);
                }
                _ => {}
            }
        }
    }

    fn glob_aggregate_load_extract(module: &Module) -> AggregateLoadExtract {
        let mut result = AggregateLoadExtract {
            map: HashMap::new(),
            indices: HashMap::new(),
        };
        Self::glob_aggregate_load_extract_in_block(&module.entry, &mut result);
        result
    }

    fn replace_aggregate_load_extract(module: &Module, ale: &AggregateLoadExtract) {
        let mut builder = IrBuilder::new(module.pools.clone());
        // hoist the indices to the top level as the indices are uniform
        let mut uniform_indices_ordering = HashMap::new();
        for (&load, extracts) in ale.map.iter() {
            if let Instruction::Call(func, args) = load.get().instruction.as_ref() {
                assert_eq!(func, &Func::Load);
                builder.set_insert_point(load);
                let loaded = args[0];
                assert_eq!(loaded.type_(), load.type_());
                for extract in extracts {
                    let indices = ale.indices.get(extract).unwrap();
                    let gep = builder.gep_chained(loaded, &indices, extract.type_().clone());
                    for index in indices.iter() {
                        let i = uniform_indices_ordering.len();
                        uniform_indices_ordering.entry(*index).or_insert(i);
                    }
                    let new_load = builder.load(gep);
                    // move the extract node to right behind the original load
                    extract.remove();
                    new_load.insert_after_self(extract.clone());
                    // replace the extract node with the new load
                    new_load.remove();
                    extract.replace_with(new_load.get());
                    // move the insert point to the new load
                    builder.set_insert_point(extract.clone());
                }
            } else {
                unreachable!();
            }
        }
        // hoist the uniform indices to the top level
        let mut uniform_indices = uniform_indices_ordering
            .iter()
            .map(|(node, _)| node.clone())
            .collect::<Vec<_>>();
        uniform_indices.sort_by_key(|node| uniform_indices_ordering.get(node).unwrap());
        for node in uniform_indices.iter().rev() {
            node.remove();
            module.entry.first.insert_after_self(node.clone());
        }
    }

    fn collect_loads_in_block(
        bb: &BasicBlock,
        loads: &mut HashSet<NodeRef>,
        all_refs: &mut HashSet<NodeRef>,
    ) {
        for node in bb.iter() {
            match node.get().instruction.as_ref() {
                Instruction::Buffer => {}
                Instruction::Bindless => {}
                Instruction::Texture2D => {}
                Instruction::Texture3D => {}
                Instruction::Accel => {}
                Instruction::Shared => {}
                Instruction::Uniform => {}
                Instruction::Local { init } => {
                    all_refs.insert(init.clone());
                }
                Instruction::Argument { .. } => {}
                Instruction::UserData(_) => {}
                Instruction::Invalid => {}
                Instruction::Const(_) => {}
                Instruction::Update { var, value } => {
                    all_refs.insert(var.clone());
                    all_refs.insert(value.clone());
                }
                Instruction::Call(func, args) => {
                    match func {
                        Func::Load | Func::ExtractElement | Func::GetElementPtr => {
                            loads.insert(node.clone());
                        }
                        _ => {}
                    }
                    for arg in args.iter() {
                        all_refs.insert(arg.clone());
                    }
                }
                Instruction::Phi(incomings) => {
                    for incoming in incomings.iter() {
                        all_refs.insert(incoming.value.clone());
                    }
                }
                Instruction::Return(value) => {
                    if value.valid() {
                        all_refs.insert(value.clone());
                    }
                }
                Instruction::Loop { body, cond } => {
                    Self::collect_loads_in_block(body, loads, all_refs);
                    all_refs.insert(cond.clone());
                }
                Instruction::GenericLoop {
                    prepare,
                    body,
                    update,
                    cond,
                } => {
                    Self::collect_loads_in_block(prepare, loads, all_refs);
                    Self::collect_loads_in_block(body, loads, all_refs);
                    Self::collect_loads_in_block(update, loads, all_refs);
                    all_refs.insert(cond.clone());
                }
                Instruction::Break => {}
                Instruction::Continue => {}
                Instruction::If {
                    cond,
                    true_branch,
                    false_branch,
                } => {
                    all_refs.insert(cond.clone());
                    Self::collect_loads_in_block(true_branch, loads, all_refs);
                    Self::collect_loads_in_block(false_branch, loads, all_refs);
                }
                Instruction::Switch {
                    value,
                    cases,
                    default,
                } => {
                    all_refs.insert(value.clone());
                    for case in cases.iter() {
                        Self::collect_loads_in_block(&case.block, loads, all_refs);
                    }
                    Self::collect_loads_in_block(default, loads, all_refs);
                }
                Instruction::AdScope { body, .. } => {
                    Self::collect_loads_in_block(body, loads, all_refs);
                }
                Instruction::RayQuery {
                    ray_query,
                    on_triangle_hit,
                    on_procedural_hit,
                } => {
                    all_refs.insert(ray_query.clone());
                    Self::collect_loads_in_block(on_triangle_hit, loads, all_refs);
                    Self::collect_loads_in_block(on_procedural_hit, loads, all_refs);
                }
                Instruction::Print { args, .. } => {
                    for arg in args.iter() {
                        all_refs.insert(arg.clone());
                    }
                }
                Instruction::AdDetach(body) => {
                    Self::collect_loads_in_block(body, loads, all_refs);
                }
                Instruction::Comment(_) => {}
                Instruction::CoroSplitMark { .. } => {}
                Instruction::CoroSuspend { .. } => {}
                Instruction::CoroResume { .. } => {}
                Instruction::CoroRegister { value, .. } => {
                    all_refs.insert(value.clone());
                }
            }
        }
    }

    fn remove_unused(module: &Module) {
        loop {
            let mut loads = HashSet::new();
            let mut all_refs = HashSet::new();
            Self::collect_loads_in_block(&module.entry, &mut loads, &mut all_refs);
            let unused = loads.difference(&all_refs).cloned().collect::<Vec<_>>();
            if unused.is_empty() {
                break;
            }
            for node in unused {
                node.remove();
            }
        }
    }

    fn transform(module: Module) -> Module {
        let aggregate_load_extract = Self::glob_aggregate_load_extract(&module);
        Self::replace_aggregate_load_extract(&module, &aggregate_load_extract);
        Self::remove_unused(&module);
        module
    }
}

impl Transform for DeferLoad {
    fn transform_module(&self, module: Module) -> Module {
        DeferLoadImpl::transform(module)
    }
}
