# SPDLog

set(SPDLOG_VERSION "v1.8.1")
set(SPDLOG_GIT_URL "https://github.com/gabime/spdlog.git")

include(ExternalProject)
find_package(Git REQUIRED)


set(SPDLOG_BASE spdlog_ep)
set(SPDLOG_PREFIX ${DEPS_PREFIX}/${SPDLOG_BASE})
set(SPDLOG_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${SPDLOG_PREFIX})
set(SPDLOG_INSTALL_DIR ${SPDLOG_BASE_DIR}/install)
set(SPDLOG_INCLUDE_DIR ${SPDLOG_INSTALL_DIR}/include)
set(SPDLOG_LIB_DIR ${SPDLOG_INSTALL_DIR}/lib)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(SPDLOG_STATIC_LIB ${SPDLOG_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}spdlogd${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
    set(SPDLOG_STATIC_LIB ${SPDLOG_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}spdlog${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()


ExternalProject_Add(${SPDLOG_BASE}
        PREFIX ${SPDLOG_BASE_DIR}
        INSTALL_DIR ${SPDLOG_INSTALL_DIR}
        GIT_REPOSITORY ${SPDLOG_GIT_URL}
        GIT_TAG ${SPDLOG_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        BUILD_BYPRODUCTS ${SPDLOG_STATIC_LIB}
        CMAKE_ARGS
        -SPDLOG_BUILD_SHARED:BOOL=OFF
        -DSPDLOG_BUILD_EXAMPLE:BOOL=OFF
        -DSPDLOG_BUILD_TESTS:BOOL=OFF
        -DSPDLOG_BUILD_BENCH:BOOL=OFF
        -DCMAKE_INSTALL_MESSAGE=NEVER
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${SPDLOG_INSTALL_DIR}
        )


file(MAKE_DIRECTORY ${SPDLOG_INCLUDE_DIR}) # Include directory needs to exist to run configure step


add_library(spdlog::spdlog STATIC IMPORTED)
set_target_properties(spdlog::spdlog PROPERTIES IMPORTED_LOCATION ${SPDLOG_STATIC_LIB})
target_include_directories(spdlog::spdlog INTERFACE ${SPDLOG_INCLUDE_DIR})
add_dependencies(spdlog::spdlog ${SPDLOG_BASE})


#showTargetProps(spdlog::spdlog)
