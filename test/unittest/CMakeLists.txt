# CONFIGURE_DEPENDS makes the build re-run the glob, so a newly added test file
# is compiled without a manual reconfigure. Without it a stale build directory
# silently keeps running the old set of tests.
file(GLOB UNIT_SOURCES CONFIGURE_DEPENDS
    "*.cpp"
)

add_executable(unittest
    ${UNIT_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../doctest_main.cpp
)

target_compile_features(unittest PRIVATE cxx_std_20)

target_include_directories(unittest PRIVATE
    ${CMAKE_SOURCE_DIR}/test
    ${CMAKE_CURRENT_SOURCE_DIR}/../common
)

target_link_libraries(unittest PRIVATE dripcore)
