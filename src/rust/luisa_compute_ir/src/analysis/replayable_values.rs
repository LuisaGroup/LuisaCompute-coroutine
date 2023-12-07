use crate::analysis::callable_arg_usages::{CallableArgumentUsageAnalysis, UsageTree};
use crate::ir::{Func, Instruction, Module, NodeRef};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) struct ReplayableValueAnalysis {
    uniform_only: bool,
    known: HashMap<NodeRef, bool>,
    args: HashMap<NodeRef, bool>,
}

impl ReplayableValueAnalysis {
    fn detect_node(&mut self, node: NodeRef) -> bool {
        if let Some(&result) = self.known.get(&node) {
            return result;
        }
        let result = match node.get().instruction.as_ref() {
            Instruction::Uniform => true,
            Instruction::Argument { by_value } => {
                *by_value || self.args.get(&node).unwrap_or(by_value).clone()
            }
            Instruction::Const(_) => true,
            Instruction::Call(func, args) => match func {
                Func::ZeroInitializer
                | Func::Unreachable(_)
                | Func::ThreadId
                | Func::BlockId
                | Func::WarpSize
                | Func::WarpLaneId
                | Func::DispatchId
                | Func::DispatchSize
                | Func::CoroId => true,
                Func::CoroToken => !self.uniform_only,
                Func::Load => {
                    false
                    // TODO: incorporate LoadUsageAnalysis?
                }
                Func::Cast
                | Func::Bitcast
                | Func::Pack
                | Func::Unpack
                | Func::Add
                | Func::Sub
                | Func::Mul
                | Func::Div
                | Func::Rem
                | Func::BitAnd
                | Func::BitOr
                | Func::BitXor
                | Func::Shl
                | Func::Shr
                | Func::RotRight
                | Func::RotLeft
                | Func::Eq
                | Func::Ne
                | Func::Lt
                | Func::Le
                | Func::Gt
                | Func::Ge
                | Func::MatCompMul
                | Func::Neg
                | Func::Not
                | Func::BitNot
                | Func::All
                | Func::Any
                | Func::Select
                | Func::Clamp
                | Func::Lerp
                | Func::Step
                | Func::SmoothStep
                | Func::Saturate
                | Func::Abs
                | Func::Min
                | Func::Max
                | Func::ReduceSum
                | Func::ReduceProd
                | Func::ReduceMin
                | Func::ReduceMax
                | Func::Clz
                | Func::Ctz
                | Func::PopCount
                | Func::Reverse
                | Func::IsInf
                | Func::IsNan
                | Func::Acos
                | Func::Acosh
                | Func::Asin
                | Func::Asinh
                | Func::Atan
                | Func::Atan2
                | Func::Atanh
                | Func::Cos
                | Func::Cosh
                | Func::Sin
                | Func::Sinh
                | Func::Tan
                | Func::Tanh
                | Func::Exp
                | Func::Exp2
                | Func::Exp10
                | Func::Log
                | Func::Log2
                | Func::Log10
                | Func::Powi
                | Func::Powf
                | Func::Sqrt
                | Func::Rsqrt
                | Func::Ceil
                | Func::Floor
                | Func::Fract
                | Func::Trunc
                | Func::Round
                | Func::Fma
                | Func::Copysign
                | Func::Cross
                | Func::Dot
                | Func::OuterProduct
                | Func::Length
                | Func::LengthSquared
                | Func::Normalize
                | Func::Faceforward
                | Func::Distance
                | Func::Reflect
                | Func::Determinant
                | Func::Transpose
                | Func::Inverse
                | Func::Vec
                | Func::Vec2
                | Func::Vec3
                | Func::Vec4
                | Func::Permute
                | Func::InsertElement
                | Func::ExtractElement
                | Func::GetElementPtr
                | Func::Struct
                | Func::Array
                | Func::Mat
                | Func::Mat2
                | Func::Mat3
                | Func::Mat4 => args.iter().all(|&arg| self.detect_node(arg)),
                _ => false,
            },
            _ => false,
        };
        self.known.insert(node, result);
        result
    }
}

impl ReplayableValueAnalysis {
    pub fn new(uniform_only: bool) -> Self {
        Self {
            uniform_only: uniform_only,
            known: HashMap::new(),
            args: HashMap::new(),
        }
    }

    pub fn new_with_module(uniform_only: bool, module: &Module) -> Self {
        let mut arg_analysis = CallableArgumentUsageAnalysis::new();
        let usage_tree = arg_analysis.analyze_module(module);
        let mut this = Self::new(uniform_only);
        for (node, _) in usage_tree.read_map {
            if !usage_tree.write_map.contains_key(&node) {
                match node.get().instruction.as_ref() {
                    Instruction::Argument { .. } => {
                        this.args.insert(node, true);
                    }
                    _ => {}
                }
            }
        }
        this
    }

    pub fn detect(&mut self, node: NodeRef) -> bool {
        self.detect_node(node)
    }
}
