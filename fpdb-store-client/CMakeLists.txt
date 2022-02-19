
#-----------------------------------------------------------------------------------------------------------------------
# Library
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb_store_client_objlib OBJECT
        src/Module.cpp include/fpdb/store/client/Module.hpp)

set_target_properties(fpdb_store_client_objlib PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(fpdb_store_client_objlib PROPERTIES OUTPUT_NAME fpdb_store_client)

find_package(LibBFD REQUIRED)

list(APPEND _PUBLIC_LINK_LIBRARIES expected)

list(APPEND _PRIVATE_LINK_LIBRARIES LibBFD)
list(APPEND _PRIVATE_LINK_LIBRARIES Backward::Backward)
list(APPEND _PRIVATE_LINK_LIBRARIES fmt::fmt)

list(APPEND _PUBLIC_SHARED_LINK_LIBRARIES arrow_flight_shared)

list(APPEND _PUBLIC_STATIC_LINK_LIBRARIES arrow_flight_static)

target_include_directories(fpdb_store_client_objlib PUBLIC include)

target_link_libraries(fpdb_store_client_objlib ${_PUBLIC_LINK_LIBRARIES})
target_link_libraries(fpdb_store_client_objlib ${_PUBLIC_SHARED_LINK_LIBRARIES})
target_link_libraries(fpdb_store_client_objlib ${_PRIVATE_LINK_LIBRARIES})

add_library(fpdb_store_client_static STATIC $<TARGET_OBJECTS:fpdb_store_client_objlib>)
set_target_properties(fpdb_store_client_static PROPERTIES OUTPUT_NAME $<TARGET_PROPERTY:fpdb_store_client_objlib,OUTPUT_NAME>)
target_include_directories(fpdb_store_client_static PUBLIC $<TARGET_PROPERTY:fpdb_store_client_objlib,INCLUDE_DIRECTORIES>)
target_link_libraries(fpdb_store_client_static PUBLIC ${_PUBLIC_LINK_LIBRARIES})
target_link_libraries(fpdb_store_client_static PRIVATE ${_PUBLIC_STATIC_LINK_LIBRARIES})
target_link_libraries(fpdb_store_client_static PRIVATE ${_PRIVATE_LINK_LIBRARIES})

add_library(fpdb_store_client_shared SHARED $<TARGET_OBJECTS:fpdb_store_client_objlib>)
set_target_properties(fpdb_store_client_shared PROPERTIES OUTPUT_NAME $<TARGET_PROPERTY:fpdb_store_client_objlib,OUTPUT_NAME>)
target_include_directories(fpdb_store_client_shared PUBLIC $<TARGET_PROPERTY:fpdb_store_client_objlib,INCLUDE_DIRECTORIES>)
target_link_libraries(fpdb_store_client_shared PUBLIC ${_PUBLIC_LINK_LIBRARIES})
target_link_libraries(fpdb_store_client_shared PRIVATE ${_PUBLIC_SHARED_LINK_LIBRARIES})
target_link_libraries(fpdb_store_client_shared PRIVATE ${_PRIVATE_LINK_LIBRARIES})
# need to fix link on mac
if (${APPLE})
    linkForMacShared(fpdb_store_client_shared)
endif()

#-----------------------------------------------------------------------------------------------------------------------
# Tests
#-----------------------------------------------------------------------------------------------------------------------

add_executable(fpdb_store_client_test
        test/Global.cpp test/Global.hpp test/Main.cpp)

target_link_libraries(fpdb_store_client_test fmt::fmt)
target_link_libraries(fpdb_store_client_test spdlog::spdlog)
target_link_libraries(fpdb_store_client_test doctest::doctest)
target_link_libraries(fpdb_store_client_test Backward::Backward)
target_link_libraries(fpdb_store_client_test fpdb_store_client_shared)
# need to add and link libraries on mac
if (${APPLE})
    linkForMacExecutable(fpdb_store_client_test)
    target_link_directories(fpdb_store_client_test PRIVATE /usr/local/opt/binutils/lib)
endif()
