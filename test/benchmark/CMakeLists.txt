file(GLOB BENCHMARK_SOURCES
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
