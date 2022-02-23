cmake_minimum_required(VERSION 3.15)
project("fpdb" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_LIST_DIR}/cmake")

include(ProcessorCount)
processorcount(NPROC)

include(CMakeUtils)

setDefaults()

include(Configure_LLVM)


#-----------------------------------------------------------------------------------------------------------------------
# Set openssl path for mac os
#-----------------------------------------------------------------------------------------------------------------------

if (${APPLE})
  set(OPENSSL_ROOT_DIR /usr/local/opt/openssl)
  # This is a bug in CMake that causes it to prefer the system version over
  # the one in the specified ROOT folder.
  set(OPENSSL_CRYPTO_LIBRARY ${OPENSSL_ROOT_DIR}/lib/libcrypto.dylib CACHE FILEPATH "" FORCE)
  set(OPENSSL_SSL_LIBRARY ${OPENSSL_ROOT_DIR}/lib/libssl.dylib CACHE FILEPATH "" FORCE)
endif()


#-----------------------------------------------------------------------------------------------------------------------
# Function for additional commands for mac os
#-----------------------------------------------------------------------------------------------------------------------

# remove /libc++fs.a from build.make and link.txt which mac does not have or need
function(removeLibcppfsLinkForMac target)
#  set(buildFile ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/${target}.dir/build.make)
#  set(linkFile ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/${target}.dir/link.txt)
#  add_custom_target(pre-compile-${target}
#          COMMAND sed -i '' '/\\/libc++fs.a/d' ${buildFile}
#          COMMAND sed -i '' 's/ \\/libc++fs.a//g' ${linkFile})
#  add_dependencies(${target} pre-compile-${target})
endfunction()

# remove -lrt from link.txt which mac does not have or need
function(removeLrtLinkForMac target)
#  set(linkFile ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/${target}.dir/link.txt)
#  add_custom_command(TARGET ${target} PRE_LINK
#          COMMAND sed -i '' 's/ -lrt//g' ${linkFile})
endfunction()

# Link missing libraries on mac
function(linkMissingLibForMac target)
#  target_link_directories(${target} PRIVATE /usr/local/opt/openssl/lib)
#  target_link_directories(${target} PRIVATE /usr/local/opt/llvm@12/lib)
#  target_link_directories(${target} PRIVATE /usr/local/opt/libdeflate/lib)
endfunction()

# fix "dyld: Library not loaded" on mac
function(fixDyldErrorForMac target output_file)
#  set(path_to_libs libgvplugin_neato_layout.6.dylib
#          libgvplugin_core.6.dylib
#          @rpath/libaws-cpp-sdk-core.dylib
#          @rpath/libaws-cpp-sdk-s3.dylib
#          @rpath/libaws-crt-cpp.dylib
#          @rpath/libcaf_core.0.18.5.dylib
#          @rpath/libcaf_io.0.18.5.dylib
#          @rpath/libcaf_openssl.0.18.5.dylib
#          @rpath/libarrow.600.dylib
#          @rpath/libarrow_flight.600.dylib)
#  set(change_libs ${CMAKE_BINARY_DIR}/_deps/graphviz_ep/install/lib/graphviz/libgvplugin_neato_layout.6.dylib
#          ${CMAKE_BINARY_DIR}/_deps/graphviz_ep/install/lib/graphviz/libgvplugin_core.6.dylib
#          ${CMAKE_BINARY_DIR}/_deps/aws-cpp-sdk_ep/install/lib/libaws-cpp-sdk-core.dylib
#          ${CMAKE_BINARY_DIR}/_deps/aws-cpp-sdk_ep/install/lib/libaws-cpp-sdk-s3.dylib
#          ${CMAKE_BINARY_DIR}/_deps/aws-cpp-sdk_ep/install/lib/libaws-crt-cpp.dylib
#          ${CMAKE_BINARY_DIR}/_deps/caf_ep/install/lib/libcaf_core.0.18.5.dylib
#          ${CMAKE_BINARY_DIR}/_deps/caf_ep/install/lib/libcaf_io.0.18.5.dylib
#          ${CMAKE_BINARY_DIR}/_deps/caf_ep/install/lib/libcaf_openssl.0.18.5.dylib
#          ${CMAKE_BINARY_DIR}/_deps/arrow_ep/install/lib/libarrow.600.dylib
#          ${CMAKE_BINARY_DIR}/_deps/arrow_ep/install/lib/libarrow_flight.600.dylib)
#  foreach(path_to_lib change_lib IN ZIP_LISTS path_to_libs change_libs)
#    add_custom_command(TARGET ${target} POST_BUILD
#            COMMAND install_name_tool -change ${path_to_lib} ${change_lib} ${output_file})
#  endforeach()
endfunction()

# Fix link issues for mac executables
function(linkForMacExecutable target)
#  removeLibcppfsLinkForMac(${target})
#  removeLrtLinkForMac(${target})
#  linkMissingLibForMac(${target})
#
#  # get name of output file
#  get_target_property(output_name ${target} OUTPUT_NAME)
#  if (${output_name} STREQUAL output_name-NOTFOUND)
#    set(output_name ${target})
#  endif()
#
#  fixDyldErrorForMac(${target} ${output_name})
endfunction()

# Fix link issues for mac shared libraries
function(linkForMacShared target)
#  removeLibcppfsLinkForMac(${target})
#  removeLrtLinkForMac(${target})
#  linkMissingLibForMac(${target})
#
#  # get name of output file
#  get_target_property(output_name ${target} OUTPUT_NAME)
#  if (${output_name} STREQUAL output_name-NOTFOUND)
#    set(output_name ${target})
#  endif()
#
#  fixDyldErrorForMac(${target} lib${output_name}.dylib)
endfunction()


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
include(External_zlib_ng)
include(External_boost)
include(External_thrift)
include(External_nlohmannjson)


#-----------------------------------------------------------------------------------------------------------------------
# Modules
#-----------------------------------------------------------------------------------------------------------------------

add_subdirectory(fpdb-util)
add_subdirectory(fpdb-caf)
add_subdirectory(fpdb-tuple)
add_subdirectory(fpdb-aws)
add_subdirectory(fpdb-catalogue)
add_subdirectory(fpdb-expression)
add_subdirectory(fpdb-expression-gandiva)
add_subdirectory(fpdb-calcite/cpp)
add_subdirectory(fpdb-plan)
add_subdirectory(fpdb-cache)
add_subdirectory(fpdb-executor)
add_subdirectory(fpdb-main)
add_subdirectory(fpdb-store-client)
add_subdirectory(fpdb-store-server)


#-----------------------------------------------------------------------------------------------------------------------
# Resources
#-----------------------------------------------------------------------------------------------------------------------

add_custom_target(resources-config
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/resources/config
        ${CMAKE_CURRENT_BINARY_DIR}/resources/config)

add_custom_target(resources-metadata
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/resources/metadata
        ${CMAKE_CURRENT_BINARY_DIR}/resources/metadata)

add_custom_target(resources-query
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/resources/query
        ${CMAKE_CURRENT_BINARY_DIR}/resources/query)

add_custom_target(resources DEPENDS resources-config resources-metadata resources-query)
