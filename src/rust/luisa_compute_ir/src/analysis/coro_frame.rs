// This file implements the coroutine frame analysis, which computes the layout of the frame structure
// and determine which fields should be loaded and saved for each subroutine.
// We use the following auxiliary sets to help the analysis:
// - Load: the set of nodes that are loaded at the beginning of the subroutine (from CoroTransferGraph)
// - Save: the set of nodes that are saved to the frame at each suspension point (from CoroTransferGraph)
// The analysis also computes a stable ordering (not changed between runs) of the nodes in the module by their
// appearing order in the subroutines sorted by the suspend token. This is important to maintain a consistent
// hash value for the generated code.

use crate::analysis::coro_graph::{CoroGraph, CoroInstrRef, CoroInstruction, CoroScopeRef};
use crate::analysis::coro_transfer_graph::CoroTransferGraph;
use crate::analysis::utility::{AccessChainIndex, AccessTree, AccessTreeNodeRef};
use crate::ir::{BasicBlock, Const, Func, Instruction, IrBuilder, NodeRef, Primitive, Type};
use crate::{CArc, TypeOf};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone)]
pub(crate) struct CoroFrameField {
    type_: Primitive,
    root: NodeRef,
    chain: Vec<usize>,
}

#[derive(Clone)]
pub(crate) struct CoroFrame<'a> {
    pub graph: &'a CoroGraph,
    pub transfer_graph: &'a CoroTransferGraph,
    pub interface_type: CArc<Type>,
    pub fields: Vec<CoroFrameField>,
}

impl<'a> CoroFrame<'a> {
    fn _try_add_child(
        &mut self,
        child_type: &Type,
        child_index: usize,
        tree_node: Option<AccessTreeNodeRef>,
        root: NodeRef,
        chain: &mut Vec<usize>,
    ) {
        chain.push(child_index);
        if let Some(tree_node) = tree_node {
            let i = AccessChainIndex::Static(child_index as i32);
            if tree_node.has_child(i) {
                self.add_node(child_type, tree_node.child(i), root, chain);
            }
        } else {
            self.add_node(child_type, None, root, chain);
        }
        let popped = chain.pop();
        assert_eq!(popped, Some(child_index));
    }

    fn add_node(
        &mut self,
        t: &Type,
        tree_node: Option<AccessTreeNodeRef>,
        root: NodeRef,
        chain: &mut Vec<usize>,
    ) {
        match t {
            Type::Void => unreachable!("void type in coroutine frame"),
            Type::UserData => unreachable!("user data type in coroutine frame"),
            Type::Primitive(p) => {
                self.fields.push(CoroFrameField {
                    type_: p.clone(),
                    root,
                    chain: chain.clone(),
                });
            }
            Type::Vector(v) => {
                let elem = v.element.to_type();
                for i in 0..v.length {
                    self._try_add_child(&elem, i as usize, tree_node, root, chain);
                }
            }
            Type::Matrix(m) => {
                let elem = m.column();
                for i in 0..m.dimension {
                    self._try_add_child(&elem, i as usize, tree_node, root, chain);
                }
            }
            Type::Struct(s) => {
                for (i, field) in s.fields.iter().enumerate() {
                    self._try_add_child(field, i, tree_node, root, chain);
                }
            }
            Type::Array(a) => {
                let elem = &a.element;
                for i in 0..a.length {
                    self._try_add_child(elem, i, tree_node, root, chain);
                }
            }
            Type::Opaque(_) => unimplemented!("opaque type in coroutine frame"),
        }
    }

    fn _build(
        graph: &'a CoroGraph,
        transfer_graph: &'a CoroTransferGraph,
        stable_indices: &HashMap<NodeRef, u32>,
        for_aggregates: bool,
    ) -> Self {
        let mut desc = CoroFrame {
            graph,
            transfer_graph,
            interface_type: CArc::null(),
            fields: Vec::new(),
        };
        for (&node, &tree_node) in transfer_graph.union_states.nodes.iter() {
            if for_aggregates == !node.type_().is_primitive() {
                desc.add_node(
                    node.type_().as_ref(),
                    Some(AccessTreeNodeRef::new(
                        &transfer_graph.union_states,
                        tree_node,
                    )),
                    node,
                    &mut Vec::new(),
                );
            }
        }
        if for_aggregates {
            desc.fields.sort_by_key(|field| {
                let node_index = stable_indices[&field.root];
                let field_size = match field.type_ {
                    Primitive::Bool => 1,
                    _ => field.type_.size() * 8,
                };
                (node_index, usize::MAX - field_size)
            });
        } else {
            desc.fields.sort_by_key(|field| {
                let node_index = stable_indices[&field.root];
                let field_size = match field.type_ {
                    Primitive::Bool => 1,
                    _ => field.type_.size() * 8,
                };
                (usize::MAX - field_size, node_index)
            });
        }
        desc
    }

    fn _compute_interface_type(&mut self) {
        let mut fields = vec![Type::vector(Primitive::Uint32, 4)];
        fields.extend(self.fields.iter().map(|field| field.type_.to_type()));
        let alignment = self
            .fields
            .iter()
            .fold(16, |acc, field| std::cmp::max(acc, field.type_.size()));
        self.interface_type = Type::struct_of(alignment as u32, fields);
    }

    fn new(
        graph: &'a CoroGraph,
        transfer_graph: &'a CoroTransferGraph,
        stable_indices: &HashMap<NodeRef, u32>,
    ) -> Self {
        let agg_desc = Self::_build(graph, transfer_graph, stable_indices, true);
        let prim_desc = Self::_build(graph, transfer_graph, stable_indices, false);
        let mut desc = CoroFrame {
            graph,
            transfer_graph,
            interface_type: CArc::null(),
            fields: [agg_desc.fields, prim_desc.fields].concat(),
        };
        desc._compute_interface_type();
        desc
    }
}

impl<'a> CoroFrame<'a> {
    pub fn resume(
        &self,
        scope: CoroScopeRef,
        frame: NodeRef,
        b: &mut IrBuilder,
    ) -> HashMap<NodeRef, NodeRef> {
        let mut mapping = HashMap::new();
        let load_tree = &self.transfer_graph.nodes[&scope].union_states_to_load;
        for (field_index, field) in self.fields.iter().enumerate() {
            let root = field.root;
            let chain: Vec<_> = field
                .chain
                .iter()
                .map(|i| AccessChainIndex::Static(*i as i32))
                .collect();
            if load_tree.contains(root, &chain) {
                let mapped = mapping
                    .entry(root)
                    .or_insert_with(|| b.local_zero_init(root.type_().clone()))
                    .clone();
                let field_index = b.const_(Const::Uint32(
                    1/* skip coro_id and token */ + field_index as u32,
                ));
                let field_type = field.type_.to_type();
                let p_value = b.gep(frame, &[field_index], field_type.clone());
                let value = b.load(p_value);
                let p_mapped = if field.chain.is_empty() {
                    mapped
                } else {
                    let chain: Vec<_> = field
                        .chain
                        .iter()
                        .map(|&i| b.const_(Const::Uint32(i as u32)))
                        .collect();
                    b.gep(mapped, chain.as_slice(), field_type.clone())
                };
                b.update(p_mapped, value);
            }
        }
        mapping
    }

    pub fn suspend(
        &self,
        scope: CoroScopeRef,
        target_token: u32,
        frame: NodeRef,
        b: &mut IrBuilder,
        mapping: &HashMap<NodeRef, NodeRef>,
    ) {
        // get the save tree
        let save_tree = &self.transfer_graph.nodes[&scope].outlets[&target_token].states_to_save;
        // update target coro token
        let t_u32 = <u32 as TypeOf>::type_();
        let target_token = b.const_(Const::Uint32(target_token));
        let zero = b.const_(Const::Zero(t_u32.clone()));
        let three = b.const_(Const::Uint32(3));
        let p_token = b.gep(frame, &[zero, three], t_u32.clone());
        b.update(p_token, target_token);
        // save fields
        for (field_index, field) in self.fields.iter().enumerate() {
            let root = field.root;
            let chain: Vec<_> = field
                .chain
                .iter()
                .map(|i| AccessChainIndex::Static(*i as i32))
                .collect();
            if save_tree.contains(root, &chain) {
                let mapped = mapping[&root].clone();
                let p_mapped = if field.chain.is_empty() {
                    mapped
                } else {
                    let chain: Vec<_> = field
                        .chain
                        .iter()
                        .map(|&i| b.const_(Const::Uint32(i as u32)))
                        .collect();
                    b.gep(mapped, chain.as_slice(), field.type_.to_type())
                };
                let value = b.load(p_mapped);
                let field_index = b.const_(Const::Uint32(
                    1/* skip coro_id and token */ + field_index as u32,
                ));
                let p_field = b.gep(frame, &[field_index], field.type_.to_type());
                b.update(p_field, value);
            }
        }
    }

    pub fn read_coro_id_and_target_token(&self, frame: NodeRef, b: &mut IrBuilder) -> NodeRef {
        let t_u32 = <u32 as TypeOf>::type_();
        let zero = b.const_(Const::Zero(t_u32.clone()));
        let gep = b.gep(frame, &[zero], Type::vector_of(t_u32.clone(), 4));
        b.load(gep)
    }

    pub fn read_coro_id(&self, frame: NodeRef, b: &mut IrBuilder) -> NodeRef {
        let t_u32 = <u32 as TypeOf>::type_();
        let coro_id_and_token = self.read_coro_id_and_target_token(frame, b);
        b.call(Func::Vec3, &[coro_id_and_token], Type::vector_of(t_u32, 3))
    }

    pub fn read_target_token(&self, frame: NodeRef, b: &mut IrBuilder) -> NodeRef {
        let t_u32 = <u32 as TypeOf>::type_();
        let zero = b.const_(Const::Zero(t_u32.clone()));
        let three = b.const_(Const::Uint32(3));
        let gep = b.gep(frame, &[zero, three], <u32 as TypeOf>::type_());
        b.load(gep)
    }

    pub fn initialize(&self, frame: NodeRef, coro_id: NodeRef, b: &mut IrBuilder) {
        let t_u32 = <u32 as TypeOf>::type_();
        let coro_id_x = b.extract(coro_id, 0, t_u32.clone());
        let coro_id_y = b.extract(coro_id, 1, t_u32.clone());
        let coro_id_z = b.extract(coro_id, 2, t_u32.clone());
        let coro_token = b.const_(Const::Uint32(u32::MAX));
        let t_u32x4 = Type::vector_of(t_u32.clone(), 4);
        let coro_id_and_token = b.call(
            Func::Vec4,
            &[coro_id_x, coro_id_y, coro_id_z, coro_token],
            t_u32x4.clone(),
        );
        let zero = b.const_(Const::Zero(t_u32.clone()));
        let gep = b.gep(frame, &[zero], t_u32x4.clone());
        b.update(gep, coro_id_and_token);
    }
}

struct CoroFrameBuilder<'a> {
    graph: &'a CoroGraph,
    transfer_graph: &'a CoroTransferGraph,
}

fn check_is_btree_map<K, V>(_: &BTreeMap<K, V>) {}

impl<'a> CoroFrameBuilder<'a> {
    fn compute_stable_node_indices(&mut self) -> HashMap<NodeRef, u32> {
        let mut nodes = HashMap::new();
        check_is_btree_map(&self.graph.tokens);
        self._collect_nodes_in_scope(self.graph.entry, &mut nodes);
        for scope in self.graph.tokens.values() {
            self._collect_nodes_in_scope(*scope, &mut nodes);
        }
        nodes
    }

    fn _collect_nodes_in_scope(&mut self, scope: CoroScopeRef, nodes: &mut HashMap<NodeRef, u32>) {
        let scope = &self.graph.get_scope(scope);
        self._collect_nodes_in_block(&scope.instructions, nodes);
    }

    fn _collect_nodes_in_block(
        &self,
        block: &Vec<CoroInstrRef>,
        nodes: &mut HashMap<NodeRef, u32>,
    ) {
        for &instr_ref in block {
            let instr = self.graph.get_instr(instr_ref);
            match instr {
                CoroInstruction::Simple(node) => {
                    self._collect_nodes_in_simple(node, nodes);
                }
                CoroInstruction::ConditionStackReplay { items } => {
                    for item in items.iter() {
                        self._collect_nodes_in_simple(&item.node, nodes);
                    }
                }
                CoroInstruction::MakeFirstFlag | CoroInstruction::ClearFirstFlag(_) => {}
                CoroInstruction::SkipIfFirstFlag { body, .. } => {
                    self._collect_nodes_in_block(body, nodes);
                }
                CoroInstruction::Loop { cond, body } => {
                    if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                        self._collect_nodes_in_simple(cond, nodes);
                    } else {
                        unreachable!("unexpected loop condition");
                    }
                    self._collect_nodes_in_block(body, nodes);
                }
                CoroInstruction::If {
                    cond,
                    true_branch,
                    false_branch,
                } => {
                    if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                        self._collect_nodes_in_simple(cond, nodes);
                    } else {
                        unreachable!("unexpected if condition");
                    }
                    self._collect_nodes_in_block(true_branch, nodes);
                    self._collect_nodes_in_block(false_branch, nodes);
                }
                CoroInstruction::Switch {
                    cond,
                    cases,
                    default,
                } => {
                    if let CoroInstruction::Simple(cond) = self.graph.get_instr(*cond) {
                        self._collect_nodes_in_simple(cond, nodes);
                    } else {
                        unreachable!("unexpected switch condition");
                    }
                    for case in cases.iter() {
                        self._collect_nodes_in_block(&case.body, nodes);
                    }
                    self._collect_nodes_in_block(default, nodes);
                }
                CoroInstruction::Suspend { .. } => {}
                CoroInstruction::Terminate => {}
                _ => unreachable!("unexpected instruction in coroutine"),
            }
        }
    }

    fn _collect_nodes_in_basic_block(&self, block: &BasicBlock, nodes: &mut HashMap<NodeRef, u32>) {
        for node in block.iter() {
            self._collect_nodes_in_simple(&node, nodes);
        }
    }

    fn _collect_nodes_in_simple(&self, simple: &NodeRef, nodes: &mut HashMap<NodeRef, u32>) {
        if nodes.contains_key(&simple) {
            return;
        }
        nodes.insert(simple.clone(), nodes.len() as u32);
        match simple.get().instruction.as_ref() {
            Instruction::Local { init } => {
                self._collect_nodes_in_simple(init, nodes);
            }
            Instruction::Update { var, value } => {
                self._collect_nodes_in_simple(var, nodes);
                self._collect_nodes_in_simple(value, nodes);
            }
            Instruction::Call(_, args) => {
                for arg in args.iter() {
                    self._collect_nodes_in_simple(arg, nodes);
                }
            }
            Instruction::Phi(incomings) => {
                for incoming in incomings.iter() {
                    self._collect_nodes_in_simple(&incoming.value, nodes);
                }
            }
            Instruction::Return(value) => {
                if value.valid() {
                    self._collect_nodes_in_simple(value, nodes);
                }
            }
            Instruction::Loop { body, cond } => {
                self._collect_nodes_in_basic_block(body, nodes);
                self._collect_nodes_in_simple(cond, nodes);
            }
            Instruction::GenericLoop {
                prepare,
                body,
                update,
                cond,
            } => {
                self._collect_nodes_in_basic_block(prepare, nodes);
                self._collect_nodes_in_basic_block(body, nodes);
                self._collect_nodes_in_basic_block(update, nodes);
                self._collect_nodes_in_simple(cond, nodes);
            }
            Instruction::If {
                cond,
                true_branch,
                false_branch,
            } => {
                self._collect_nodes_in_simple(cond, nodes);
                self._collect_nodes_in_basic_block(true_branch, nodes);
                self._collect_nodes_in_basic_block(false_branch, nodes);
            }
            Instruction::Switch {
                value,
                default,
                cases,
            } => {
                self._collect_nodes_in_simple(value, nodes);
                self._collect_nodes_in_basic_block(default, nodes);
                for case in cases.iter() {
                    self._collect_nodes_in_basic_block(&case.block, nodes);
                }
            }
            Instruction::AdScope { body, .. } => {
                self._collect_nodes_in_basic_block(body, nodes);
            }
            Instruction::RayQuery {
                ray_query,
                on_triangle_hit,
                on_procedural_hit,
            } => {
                self._collect_nodes_in_simple(ray_query, nodes);
                self._collect_nodes_in_basic_block(on_triangle_hit, nodes);
                self._collect_nodes_in_basic_block(on_procedural_hit, nodes);
            }
            Instruction::Print { args, .. } => {
                for arg in args.iter() {
                    self._collect_nodes_in_simple(arg, nodes);
                }
            }
            Instruction::AdDetach(body) => {
                self._collect_nodes_in_basic_block(body, nodes);
            }
            Instruction::CoroRegister { value, .. } => {
                self._collect_nodes_in_simple(value, nodes);
            }
            _ => {}
        }
    }
}

impl<'a> CoroFrameBuilder<'a> {
    fn build(graph: &'a CoroGraph, transfer_graph: &'a CoroTransferGraph) -> CoroFrame<'a> {
        let mut builder = CoroFrameBuilder {
            graph,
            transfer_graph,
        };
        let stable_node_indices = builder.compute_stable_node_indices();
        CoroFrame::new(graph, transfer_graph, &stable_node_indices)
    }
}

impl<'a> CoroFrame<'a> {
    pub fn build(graph: &'a CoroGraph, transfer_graph: &'a CoroTransferGraph) -> CoroFrame<'a> {
        CoroFrameBuilder::build(graph, transfer_graph)
    }
}
