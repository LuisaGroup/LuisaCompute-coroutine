//
// Created by Mike on 2024/5/8.
//

#include <luisa/core/logging.h>
#include <luisa/ast/function_builder.h>
#include <luisa/ir/ir2ast.h>
#include <luisa/coro/coro_frame_desc.h>
#include <luisa/coro/coro_graph.h>

#ifdef LUISA_ENABLE_IR
#include <luisa/ir/ast2ir.h>
#endif

namespace luisa::compute::coroutine {

CoroGraph::Node::Node(luisa::vector<uint> input_fields,
                      luisa::vector<uint> output_fields,
                      luisa::vector<CoroToken> targets,
                      CC current_continuation) noexcept
    : _input_fields{std::move(input_fields)},
      _output_fields{std::move(output_fields)},
      _targets{std::move(targets)},
      _cc{std::move(current_continuation)} {}

CoroGraph::Node::~Node() noexcept = default;

Function CoroGraph::Node::cc() const noexcept { return _cc->function(); }

luisa::string CoroGraph::Node::dump() const noexcept {
    luisa::string s;
    s.append("  Input Fields: [");
    for (auto i : _input_fields) {
        s.append(luisa::format("{}, ", i));
    }
    if (!_input_fields.empty()) {
        s.pop_back();
        s.pop_back();
    }
    s.append("]\n");
    s.append("  Output Fields: [");
    for (auto i : _output_fields) {
        s.append(luisa::format("{}, ", i));
    }
    if (!_output_fields.empty()) {
        s.pop_back();
        s.pop_back();
    }
    s.append("]\n");
    s.append("  Transition Targets: [");
    for (auto i : _targets) {
        s.append(luisa::format("{}, ", i));
    }
    if (!_targets.empty()) {
        s.pop_back();
        s.pop_back();
    }
    s.append("]\n");
    return s;
}

CoroGraph::CoroGraph(luisa::shared_ptr<const CoroFrameDesc> frame_desc,
                     luisa::unordered_map<CoroToken, Node> nodes,
                     luisa::unordered_map<luisa::string, CoroToken> named_tokens) noexcept
    : _frame{std::move(frame_desc)},
      _nodes{std::move(nodes)},
      _named_tokens{std::move(named_tokens)} {}

CoroGraph::~CoroGraph() noexcept = default;

const CoroGraph::Node &CoroGraph::entry() const noexcept {
    return node(coro_token_entry);
}

const CoroGraph::Node &CoroGraph::node(CoroToken token) const noexcept {
    auto iter = _nodes.find(token);
    LUISA_ASSERT(iter != _nodes.end(),
                 "Coroutine node with token {} not found.",
                 token);
    return iter->second;
}

const CoroGraph::Node &CoroGraph::node(luisa::string_view name) const noexcept {
    auto iter = _named_tokens.find(name);
    LUISA_ASSERT(iter != _named_tokens.end(),
                 "Coroutine node with name '{}' not found.",
                 name);
    return node(iter->second);
}

luisa::string CoroGraph::dump() const noexcept {
    luisa::string s;
    s.append("Arguments:\n");
    auto args = entry().cc().arguments();
    for (auto i = 0u; i < args.size(); i++) {
        s.append(luisa::format("  Argument {}: ", i));
        s.append(args[i].type()->description());
        if (args[i].is_reference()) { s.append(" &"); }
        s.append("\n");
    }
    s.append("Frame:\n").append(_frame->dump());
    for (auto &&[token, node] : _nodes) {
        if (token == coro_token_entry) {
            s.append("Entry:\n");
        } else {
            s.append(luisa::format("Node {}:\n", token));
        }
        s.append(node.dump());
    }
    if (!_named_tokens.empty()) {
        s.append("Named Tokens:\n");
        for (auto &&[name, token] : _named_tokens) {
            s.append(luisa::format("  {} -> \"{}\"\n", token, name));
        }
    }
    return s;
}

#ifndef LUISA_ENABLE_IR
luisa::shared_ptr<const CoroGraph> CoroGraph::create(Function coroutine) noexcept {
    LUISA_ERROR_WITH_LOCATION(
        "Coroutine requires IR support but "
        "LuisaCompute is built without the IR module. "
        "This might be caused by missing Rust. "
        "Please install the Rust toolchain and "
        "recompile LuisaCompute to get the IR module.");
}
#else

namespace detail {

static void perform_coroutine_transform(ir::CallableModule *m) noexcept {
    auto coroutine_pipeline = ir::luisa_compute_ir_transform_pipeline_new();
    // ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "canonicalize_control_flow");
    // ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "demote_locals");
    // ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "defer_load");
    // ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "extract_loop_cond");
    // ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "split_coro");
    ir::luisa_compute_ir_transform_pipeline_add_transform(coroutine_pipeline, "materialize_coro_v2");
    auto converted_module = ir::luisa_compute_ir_transform_pipeline_transform_callable(coroutine_pipeline, *m);
    ir::luisa_compute_ir_transform_pipeline_destroy(coroutine_pipeline);
    *m = converted_module;
}

[[nodiscard]] static auto make_subroutine_wrapper(Function coroutine, Function cc) noexcept {
    using FB = luisa::compute::detail::FunctionBuilder;
    return FB::define_callable([&] {
        luisa::vector<const Expression *> args;
        args.reserve(1u /* frame */ + coroutine.arguments().size());
        LUISA_ASSERT(coroutine.arguments().size() == coroutine.bound_arguments().size(),
                     "Invalid capture list size (expected {}, got {}).",
                     coroutine.arguments().size(), coroutine.bound_arguments().size());
        auto fb = FB::current();
        args.emplace_back(fb->reference(cc.arguments().front().type()));
        for (auto arg_i = 0u; arg_i < coroutine.arguments().size(); arg_i++) {
            auto def_arg = coroutine.arguments()[arg_i];
            auto internal_arg = luisa::visit(
                [&]<typename T>(T b) noexcept -> const Expression * {
                    if constexpr (std::is_same_v<T, Function::BufferBinding>) {
                        return fb->buffer_binding(def_arg.type(), b.handle, b.offset, b.size);
                    } else if constexpr (std::is_same_v<T, Function::TextureBinding>) {
                        return fb->texture_binding(def_arg.type(), b.handle, b.level);
                    } else if constexpr (std::is_same_v<T, Function::BindlessArrayBinding>) {
                        return fb->bindless_array_binding(b.handle);
                    } else if constexpr (std::is_same_v<T, Function::AccelBinding>) {
                        return fb->accel_binding(b.handle);
                    } else {
                        static_assert(std::is_same_v<T, luisa::monostate>);
                        switch (def_arg.tag()) {
                            case Variable::Tag::REFERENCE: return fb->reference(def_arg.type());
                            case Variable::Tag::BUFFER: return fb->buffer(def_arg.type());
                            case Variable::Tag::TEXTURE: return fb->texture(def_arg.type());
                            case Variable::Tag::BINDLESS_ARRAY: return fb->bindless_array();
                            case Variable::Tag::ACCEL: return fb->accel();
                            default: /* value argument */ return fb->argument(def_arg.type());
                        }
                    }
                },
                coroutine.bound_arguments()[arg_i]);
            args.emplace_back(internal_arg);
        }
        LUISA_ASSERT(cc.return_type() == nullptr,
                     "Coroutine subroutines should not have return type.");
        fb->call(cc, args);
    });
}

}// namespace detail

luisa::shared_ptr<const CoroGraph> CoroGraph::create(Function coroutine) noexcept {
    LUISA_VERBOSE_WITH_LOCATION("Performing Coroutine transform "
                                "on function with hash {:016x}.",
                                coroutine.hash());

    // convert the coroutine function to IR, transform it, and then convert back
    auto m = AST2IR::build_coroutine(coroutine);
    detail::perform_coroutine_transform(m->get());
    auto entry = IR2AST::build(m->get());

    // create the coroutine frame descriptor
    auto frame = [m, entry] {
        auto underlying = entry->arguments().front().type();
        CoroFrameDesc::DesignatedFieldDict members;
        for (auto &&field : luisa::span{m->get()->coro_frame_designated_fields.ptr,
                                        m->get()->coro_frame_designated_fields.len}) {
            auto name = luisa::string_view{reinterpret_cast<const char *>(field.name.ptr), field.name.len};
            if (!name.empty() && name.back() == '\0') { name = name.substr(0, name.size() - 1); }
            auto [_, success] = members.try_emplace(name, field.index);
            LUISA_ASSERT(success, "Duplicated designated field name '{}' at field {}.", name, field.index);
        }
        return CoroFrameDesc::create(underlying, std::move(members));
    }();

    // extract the subroutines
    auto subroutines = m->get()->subroutines;
    auto subroutine_ids = m->get()->subroutine_ids;
    LUISA_ASSERT(subroutines.len == subroutine_ids.len,
                 "Subroutine count mismatch: {} vs {}.",
                 subroutines.len, subroutine_ids.len);
    luisa::unordered_map<CoroToken, Node> nodes;
    nodes.reserve(subroutines.len + 1u);
    auto convert_fields = [](ir::CBoxedSlice<uint> slice) noexcept {
        luisa::vector<uint> fields;
        fields.reserve(slice.len);
        for (auto i = 0u; i < slice.len; i++) { fields.emplace_back(slice.ptr[i]); }
        return fields;
    };
    // add the entry node
    nodes.emplace(
        coro_token_entry,
        Node{convert_fields(m->get()->coro_frame_input_fields),
             convert_fields(m->get()->coro_frame_output_fields),
             convert_fields(m->get()->coro_target_tokens),
             detail::make_subroutine_wrapper(coroutine, entry->function())});
    // add subroutine nodes
    for (auto i = 0u; i < subroutines.len; i++) {
        auto s = subroutines.ptr[i]._0.get();
        auto subroutine = IR2AST::build(s);
        auto [_, success] = nodes.try_emplace(
            subroutine_ids.ptr[i],
            Node{convert_fields(s->coro_frame_input_fields),
                 convert_fields(s->coro_frame_output_fields),
                 convert_fields(s->coro_target_tokens),
                 detail::make_subroutine_wrapper(coroutine, subroutine->function())});
        LUISA_ASSERT(success, "Duplicated subroutine token {}.", subroutine_ids.ptr[i]);
    }
    // create the graph
    return luisa::make_shared<CoroGraph>(
        std::move(frame), std::move(nodes),
        coroutine.builder()->coro_tokens());
}

#endif

}// namespace luisa::compute::coroutine
