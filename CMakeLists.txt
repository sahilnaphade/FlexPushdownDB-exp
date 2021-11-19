cmake_minimum_required(VERSION 3.15)
project("normal" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_LIST_DIR}/cmake")

include(CMakeUtils)

setDefaults()

include(Configure_LLVM)


#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------

set(DEPS_PREFIX "_deps")

include(External_spdlog)
include(External_doctest)
include(External_nanobench)
include(External_caf)
include(External_arrow)
include(External_AWSCPPSDK)
include(External_ANTLR)
include(External_graphviz)
include(External_expected)
include(External_fmt)
include(External_ssb_dbgen)
include(External_primesieve)
include(Dependency_SQLite3)
include(External_Backward)
include(External_CallableTraits)
include(External_boost)
include(External_thrift)
include(External_nlohmannjson)


#-----------------------------------------------------------------------------------------------------------------------
# Modules
#-----------------------------------------------------------------------------------------------------------------------

add_subdirectory(normal-util)
add_subdirectory(normal-tuple)
add_subdirectory(normal-aws)
add_subdirectory(normal-catalogue)
add_subdirectory(normal-expression)
add_subdirectory(normal-expression-gandiva)
add_subdirectory(normal-calcite/cpp)
add_subdirectory(normal-plan)
add_subdirectory(normal-cache)
add_subdirectory(normal-executor)
add_subdirectory(normal-frontend)
add_subdirectory(normal-sql)
add_subdirectory(normal-ssb)


#-----------------------------------------------------------------------------------------------------------------------
# Resources
#-----------------------------------------------------------------------------------------------------------------------

file(COPY resources DESTINATION ${CMAKE_CURRENT_BINARY_DIR})