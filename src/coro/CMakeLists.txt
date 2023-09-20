set(LUISA_COMPUTE_CORO_SOURCES
        shader_scheduler.cpp
        coro_dispatcher.cpp
        coro_graph.cpp
        coro_node.cpp)

add_library(luisa-compute-coro SHARED ${LUISA_COMPUTE_CORO_SOURCES})
target_link_libraries(luisa-compute-coro PUBLIC luisa-compute-dsl)
target_precompile_headers(luisa-compute-coro PRIVATE pch.h)

target_compile_definitions(luisa-compute-coro PRIVATE LC_CORO_EXPORT_DLL=1)
set_target_properties(luisa-compute-coro PROPERTIES
        UNITY_BUILD ${LUISA_COMPUTE_ENABLE_UNITY_BUILD}
        OUTPUT_NAME lc-coro)

luisa_compute_install(coro SOURCES ${LUISA_COMPUTE_CORO_SOURCES})
