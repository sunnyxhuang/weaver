cmake_minimum_required(VERSION 3.6)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

add_executable(ximulator_main
        main.cc)
target_link_libraries(ximulator_main
        ximulator)

add_library(ximulator STATIC
        events.cc)
target_link_libraries(ximulator
        scheduler
        traffic_gen)

add_library(scheduler STATIC
        scheduler.cc
        schedulerAalo.cc
        schedulerHybrid.cc
        schedulerInfocom.cc
        schedulerWeaver.cc
        schedulerVarys.cc)
target_link_libraries(scheduler
        coflow
        db_logger
        global
        solver_infocom
        ximulator)

add_library(traffic_gen STATIC
        traffic_generator.cc)
target_link_libraries(traffic_gen
        coflow
        db_logger
        global
        ximulator)

add_library(global STATIC
        global.cc)
target_link_libraries(global
        util)

add_library(util STATIC
        util.cc)

add_library(coflow STATIC
        coflow.cc)
target_link_libraries(coflow
        global
        util)

if (UNIX AND NOT APPLE)
    # include directory for remote linux nodes
    include_directories(
            /usr/include/mysql
            /usr/include/mysql++)
elseif (APPLE)
    # library under local macbook
    include_directories(
            /usr/local/include/mysql
            /usr/local/include/mysql++)
endif ()

add_library(db_logger
        db_logger.cc)
target_link_libraries(db_logger
        coflow
        util)

# Gurobi-depending libraries
set(CMAKE_SHARED_LINKER_FLAGS "-lpthread -lm")
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

find_library(GUROBI_LIBRARY
        NAMES libgurobi70.so
        PATHS ${GUROBI_DIRS}
        NO_DEFAULT_PATH)

find_library(GUROBI_CXX_LIBRARY
        NAMES libgurobi_c++.a
        PATHS ${GUROBI_DIRS}
        NO_DEFAULT_PATH)

MESSAGE(STATUS "GUROBI_DIRS = ${GUROBI_DIRS}")
MESSAGE(STATUS "GUROBI_INCLUDE_DIR = ${GUROBI_INCLUDE_DIR}")
MESSAGE(STATUS "GUROBI_LIBRARY = ${GUROBI_LIBRARY}")
MESSAGE(STATUS "GUROBI_CXX_LIBRARY = ${GUROBI_CXX_LIBRARY}")

include_directories(${GUROBI_INCLUDE_DIR})
add_library(solver_infocom solver_infocom.cc)
target_link_libraries(solver_infocom
        ${GUROBI_LIBRARY} ${GUROBI_CXX_LIBRARY})

