# Thrift

set(THRIFT_VERSION "0.15.0")
set(THRIFT_GIT_URL "https://github.com/apache/thrift.git")

include(ExternalProject)
find_package(Git REQUIRED)

set(THRIFT_BASE thrift_ep)
set(THRIFT_PREFIX ${DEPS_PREFIX}/${THRIFT_BASE})
set(THRIFT_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${THRIFT_PREFIX})
set(THRIFT_INSTALL_DIR ${THRIFT_BASE_DIR}/install)
set(THRIFT_INCLUDE_DIR ${THRIFT_INSTALL_DIR}/include)
set(THRIFT_LIB_DIR ${THRIFT_INSTALL_DIR}/lib)
if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  set(THRIFT_STATIC_LIB ${THRIFT_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}thriftd${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
  set(THRIFT_STATIC_LIB ${THRIFT_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}thrift${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()

ExternalProject_Add(${THRIFT_BASE}
        PREFIX ${THRIFT_BASE_DIR}
        GIT_REPOSITORY ${THRIFT_GIT_URL}
        GIT_TAG ${THRIFT_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        INSTALL_DIR ${THRIFT_INSTALL_DIR}
        BUILD_BYPRODUCTS ${THRIFT_STATIC_LIB}
        CMAKE_ARGS
        -DOPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include
        -DOPENSSL_CRYPTO_LIBRARY=/usr/local/opt/openssl/lib/libcrypto.dylib
        -DOPENSSL_SSL_LIBRARY=/usr/local/opt/openssl/lib/libssl.dylib
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=-isystem\ ${BOOST_INCLUDE_DIR}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_MESSAGE=NEVER
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${THRIFT_INSTALL_DIR}
        )


file(MAKE_DIRECTORY ${THRIFT_INCLUDE_DIR}) # Include directory needs to exist to run configure step

add_library(thrift_static STATIC IMPORTED)
set_target_properties(thrift_static PROPERTIES IMPORTED_LOCATION ${THRIFT_STATIC_LIB})
target_include_directories(thrift_static INTERFACE ${THRIFT_INCLUDE_DIR})
add_dependencies(thrift_static ${THRIFT_BASE})


#showTargetProps(thrift::thrift)
