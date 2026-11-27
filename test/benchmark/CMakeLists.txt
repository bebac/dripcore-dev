# CONFIGURE_DEPENDS makes the build re-run the glob, so a newly added benchmark
# file is compiled without a manual reconfigure. Without it a stale build
# directory silently keeps running the old set of benchmarks.
file(GLOB BENCHMARK_SOURCES CONFIGURE_DEPENDS
    "*.cpp"
)

add_executable(benchmark
    ${BENCHMARK_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../doctest_main.cpp
)

target_compile_features(benchmark PRIVATE cxx_std_20)

target_include_directories(benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/test
    ${CMAKE_CURRENT_SOURCE_DIR}/../common
)

target_link_libraries(benchmark PRIVATE dripcore)
