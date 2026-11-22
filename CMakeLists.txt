cmake_minimum_required(VERSION 3.15)
project(dripcore VERSION 0.1.0 LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# Detect platform and select appropriate event loop implementation
if(APPLE)
    set(EVENT_LOOP_SOURCE lib/dripcore/source/event_loop_kqueue.cpp)
elseif(UNIX)
    set(EVENT_LOOP_SOURCE lib/dripcore/source/event_loop_epoll.cpp)
else()
    message(FATAL_ERROR "Unsupported platform")
endif()

# Define the Library
add_library(dripcore STATIC
    lib/dripcore/source/task_handle.cpp
    lib/dripcore/source/executor.cpp
    lib/dripcore/source/scheduler.cpp
    lib/dripcore/source/yield_awaiter.cpp
    lib/dripcore/source/io_awaiter.cpp
    lib/dripcore/source/socket.cpp
    lib/dripcore/source/sleep_awaiter.cpp
    lib/dripcore/source/blocking_pool.cpp
    lib/dripcore/source/task_control.cpp
    ${EVENT_LOOP_SOURCE}
)

# Set up Include Paths
target_include_directories(dripcore
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/lib/dripcore/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        lib/dripcore/source
)

target_compile_features(dripcore PRIVATE cxx_std_20)

set_target_properties(dripcore PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION TRUE
)

option(BUILD_TESTING "Build the testing suite" OFF)
option(BUILD_BENCHMARKS "Build the benchmark suite" OFF)
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(test/unittest)
endif()

if(BUILD_BENCHMARKS)
    add_subdirectory(test/benchmark)
endif()

# Examples
add_subdirectory(examples)

if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

if(ENABLE_TSAN)
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()

# Main application
add_executable(dripcore-cli
    src/main.cpp
)

target_compile_features(dripcore-cli PRIVATE cxx_std_20)

target_include_directories(dripcore-cli
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(dripcore-cli PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION TRUE
)

target_link_libraries(dripcore-cli PRIVATE dripcore)

