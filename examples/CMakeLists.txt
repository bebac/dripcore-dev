# Pipeline example — three-stage text processing pipeline using channels
add_executable(pipeline pipeline.cpp)

target_compile_features(pipeline PRIVATE cxx_std_20)

target_link_libraries(pipeline PRIVATE dripcore)