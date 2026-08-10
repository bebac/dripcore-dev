file(GLOB UNIT_SOURCES
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
