# To enable std::filesystem support programs need to link against extra libraries
# As std::filesystem is an emerging standard, this is different for different versions
# of LLVM and different platforms.
#
# Not entirely clear what is the correct way to do this, but below we create a dummy library for the library containing
# filesystem support. This works on mac. The linux version is linking to the gcc stdlib which contains filesystem
# support. FIXME: Get it linking properly to the LLVM lib.

#set(LLVM_ROOT /usr/local/opt/llvm@12)
#find_package(LLVM)

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
#    add_library(LLVM-filesystem STATIC IMPORTED)
#    get_filename_component(LLVM_FS_LIB ${LLVM_DIR}/../../${CMAKE_STATIC_LIBRARY_PREFIX}c++fs${CMAKE_STATIC_LIBRARY_SUFFIX} REALPATH)
#    set_target_properties(LLVM-filesystem PROPERTIES IMPORTED_LOCATION ${LLVM_FS_LIB})
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
#    add_library(LLVM-filesystem STATIC IMPORTED)
#    get_filename_component(LLVM_FS_LIB ${LLVM_DIR}/../lib/${CMAKE_STATIC_LIBRARY_PREFIX}c++fs${CMAKE_STATIC_LIBRARY_SUFFIX} REALPATH)
#    set_target_properties(LLVM-filesystem PROPERTIES IMPORTED_LOCATION ${LLVM_FS_LIB})

    add_library(LLVM-filesystem INTERFACE)
    target_link_libraries(LLVM-filesystem INTERFACE stdc++fs)
else()
    message(FATAL ERROR "Platform ${CMAKE_SYSTEM_NAME} not yet supported")
endif()

#showTargetProps(LLVM::fs)

# Search for LLVM
set(LLVM_HINTS ${LLVM_ROOT} ${LLVM_DIR} /usr/lib /usr/share)
if(_LLVM_DIR)
    list(APPEND LLVM_HINTS ${_LLVM_DIR})
endif()
foreach (FPDB_LLVM_VERSION ${FPDB_LLVM_VERSIONS})
    find_package(LLVM
            ${FPDB_LLVM_VERSION}
            QUIET
            CONFIG
            HINTS
            ${LLVM_HINTS})
    if (LLVM_FOUND)
        break()
    endif ()
endforeach ()


# Define LLVM targets
if (LLVM_FOUND)

    message(STATUS "Found acceptable LLVM version ${LLVM_VERSION} ${LLVM_DIR}")

    # Find the libraries that correspond to the LLVM components
    llvm_map_components_to_libnames(LLVM_LIBS
            core
            mcjit
            native
            ipo
            bitreader
            target
            linker
            analysis
            debuginfodwarf)

    find_program(LLVM_LINK_EXECUTABLE llvm-link HINTS ${LLVM_TOOLS_BINARY_DIR})

    find_program(CLANG_EXECUTABLE
            NAMES clang-${LLVM_PACKAGE_VERSION}
            clang-${LLVM_VERSION_MAJOR}.${LLVM_VERSION_MINOR}
            clang-${LLVM_VERSION_MAJOR} clang
            HINTS ${LLVM_TOOLS_BINARY_DIR})

    add_library(LLVM::LLVM_INTERFACE INTERFACE IMPORTED)

    set_target_properties(LLVM::LLVM_INTERFACE
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${LLVM_INCLUDE_DIRS}"
            INTERFACE_COMPILE_FLAGS "${LLVM_DEFINITIONS}"
            INTERFACE_LINK_LIBRARIES "${LLVM_LIBS}")
else ()
    message(FATAL_ERROR "Could not find acceptable LLVM version. Acceptable versions: ${FPDB_LLVM_VERSIONS}")
endif ()

mark_as_advanced(CLANG_EXECUTABLE LLVM_LINK_EXECUTABLE)

find_package_handle_standard_args(LLVMAlt
        REQUIRED_VARS # The first variable is used for display.
        LLVM_PACKAGE_VERSION
        CLANG_EXECUTABLE
        LLVM_FOUND
        LLVM_LINK_EXECUTABLE)
if (LLVMAlt_FOUND)
    message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
    message(STATUS "Found llvm-link ${LLVM_LINK_EXECUTABLE}")
    message(STATUS "Found clang ${CLANG_EXECUTABLE}")
endif ()
