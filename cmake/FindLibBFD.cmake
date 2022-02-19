include(FindPackageHandleStandardArgs)
include(GNUInstallDirs)

find_path(LibBFD_INCLUDE_DIR NAMES
        "bfd.h"
        PATHS /usr/${CMAKE_INSTALL_INCLUDEDIR} /usr/local/opt/binutils/${CMAKE_INSTALL_INCLUDEDIR})

find_library(LibBFD_LIBRARY
        bfd
        PATHS /usr/${CMAKE_INSTALL_LIBDIR} /usr/local/opt/binutils/${CMAKE_INSTALL_LIBDIR})

set(LibBFD_INCLUDE_DIRS ${LibBFD_INCLUDE_DIR} ${LibDL_INCLUDE_DIR})
set(LibBFD_LIBRARIES ${LibBFD_LIBRARY} ${LibDL_LIBRARY})

find_package_handle_standard_args(LibBFD ${_name_mismatched_arg}
        REQUIRED_VARS LibBFD_LIBRARY LibBFD_INCLUDE_DIR)

mark_as_advanced(LibBFD_INCLUDE_DIR LibBFD_LIBRARY)

add_library(LibBFD SHARED IMPORTED)
set_target_properties(LibBFD PROPERTIES IMPORTED_LOCATION ${LibBFD_LIBRARY})
target_include_directories(LibBFD INTERFACE ${LibBFD_INCLUDE_DIR})
target_link_libraries(LibBFD INTERFACE ${CMAKE_DL_LIBS})

#message(STATUS ${LibBFD_LIBRARY})
#message(STATUS ${LibBFD_INCLUDE_DIR})