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

# Detect platform and select appropriate event loop implementation
if(APPLE)
    set(EVENT_LOOP_SOURCE lib/dripcore/source/event_loop_kqueue.cpp)
elseif(UNIX)
    set(EVENT_LOOP_SOURCE lib/dripcore/source/event_loop_epoll.cpp)
else()
    message(FATAL_ERROR "Unsupported platform")
endif()

add_library(core STATIC
    lib/dripcore/source/task_handle.cpp
    lib/dripcore/source/executor.cpp
    lib/dripcore/source/scheduler.cpp
    lib/dripcore/source/yield_awaiter.cpp
    lib/dripcore/source/io_awaiter.cpp
    lib/dripcore/source/socket.cpp
    lib/dripcore/source/sleep_awaiter.cpp
    lib/dripcore/source/task_control.cpp
    ${EVENT_LOOP_SOURCE}
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
# Test Infrastructure
# -----------------------------------------------

# Include test subdirectories
add_subdirectory(test/unittest)
add_subdirectory(test/benchmark)

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

add_test(NAME unittest COMMAND test/unittest/unittest)
add_test(NAME benchmark COMMAND test/benchmark/benchmark)

