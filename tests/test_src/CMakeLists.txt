set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(gtest_SOURCE_DIR
        ../test_lib/googletest)
set(ximulator_SOURCE_DIR
        ../..)

include_directories(
        ${gtest_SOURCE_DIR}/googlemock/include
        ${gtest_SOURCE_DIR}/googletest/include
        ${gtest_SOURCE_DIR}
        ${ximulator_SOURCE_DIR})

add_executable(ximulator_test
        ximulator_test.cc)
target_link_libraries(ximulator_test
        gtest
        gtest_main
        ximulator)

add_executable(ximulator_hpns_test
        ximulator_hpns_test.cc)
target_link_libraries(ximulator_hpns_test
        gtest
        gtest_main
        ximulator)

add_executable(traffic_generator_test
        traffic_generator_test.cc)
target_link_libraries(traffic_generator_test
        gtest
        gtest_main
        ximulator)

if (UNIX AND NOT APPLE)
    set(GUROBI_DIRS
            "${PROJECT_SOURCE_DIR}/../gurobi/linux64"
            "${PROJECT_SOURCE_DIR}/gurobi/linux64")
elseif (APPLE)
    set(GUROBI_DIRS
            "${PROJECT_SOURCE_DIR}/../gurobi/mac64"
            "${PROJECT_SOURCE_DIR}/gurobi/mac64")
endif ()

find_path(GUROBI_INCLUDE_DIR
        NAMES gurobi_c++.h
        PATHS ${GUROBI_DIRS}
        NO_DEFAULT_PATH)
add_executable(solver_test solver_test.cc)
target_link_libraries(solver_test
        gtest
        gtest_main
        ximulator)
include_directories(${GUROBI_INCLUDE_DIR})