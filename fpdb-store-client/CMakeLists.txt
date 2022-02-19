
#-----------------------------------------------------------------------------------------------------------------------
# Library
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb-store-client-objlib OBJECT
        src/Module.cpp include/fpdb/store/client/Module.hpp)

set_target_properties(fpdb-store-client-objlib PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(fpdb-store-client-objlib PROPERTIES OUTPUT_NAME fpdb-store-client)

target_include_directories(fpdb-store-client-objlib PUBLIC include)

add_library(fpdb-store-client-static STATIC $<TARGET_OBJECTS:fpdb-store-client-objlib>)
set_target_properties(fpdb-store-client-static PROPERTIES OUTPUT_NAME $<TARGET_PROPERTY:fpdb-store-client-objlib,OUTPUT_NAME>)
target_include_directories(fpdb-store-client-static PUBLIC $<TARGET_PROPERTY:fpdb-store-client-objlib,INCLUDE_DIRECTORIES>)

add_library(fpdb-store-client-shared SHARED $<TARGET_OBJECTS:fpdb-store-client-objlib>)
set_target_properties(fpdb-store-client-shared PROPERTIES OUTPUT_NAME $<TARGET_PROPERTY:fpdb-store-client-objlib,OUTPUT_NAME>)
target_include_directories(fpdb-store-client-shared PUBLIC $<TARGET_PROPERTY:fpdb-store-client-objlib,INCLUDE_DIRECTORIES>)


#-----------------------------------------------------------------------------------------------------------------------
# Tests
#-----------------------------------------------------------------------------------------------------------------------

add_executable(fpdb-store-client-test
        test/Module.cpp test/Module.hpp)

target_link_libraries(fpdb-store-client-test fpdb-store-client-shared)
# need to add and link libraries on mac
if (${APPLE})
    linkForMacExecutable(fpdb-store-client-test)
endif()
