cmake_minimum_required(VERSION 3.10)
project(DripcoreDev)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 1. Force a default Build Type to Debug if none is specified
#if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
#  message(STATUS "Setting build type to 'Debug' as none was specified.")
#  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose the type of build." FORCE)
#endif()

# 2. Add specific flags for GCC/Clang to ensure clean backtraces
#if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
#    add_compile_options(-g -Og -fno-omit-frame-pointer)
#endif()

# -----------------------------------------------
# Core library
# -----------------------------------------------

add_library(core STATIC
    lib/dripcore/source/task.cpp
    lib/dripcore/source/scheduler.cpp
    lib/dripcore/source/yield_awaiter.cpp
    lib/dripcore/source/io_awaiter.cpp
)

target_compile_features(core PRIVATE cxx_std_20)

target_include_directories(core
    #PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/lib/dripcore/include
)

set_target_properties(core PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION TRUE
)

# -----------------------------------------------
# Test
# -----------------------------------------------

# Find and include all .cpp files in the ./test directory
file(GLOB TEST_SOURCES "test/*.cpp")

add_executable(doctest
    ${TEST_SOURCES}
)

target_compile_features(doctest PRIVATE cxx_std_20)

target_link_libraries(doctest PRIVATE core)

set_target_properties(doctest PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION TRUE
)

# -----------------------------------------------
# Main application
# -----------------------------------------------
add_executable(dripcore
    src/main.cpp
)

target_compile_features(dripcore PRIVATE cxx_std_20)

target_include_directories(dripcore
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(dripcore PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION TRUE
)

target_link_libraries(dripcore PRIVATE core)

# -----------------------------------------------
# Enable testing
# -----------------------------------------------
include(CTest)
add_test(NAME run_tests COMMAND doctest)
