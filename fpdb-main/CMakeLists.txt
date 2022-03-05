project(fpdb-main VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Targets
#-----------------------------------------------------------------------------------------------------------------------

# fpdb-main-client
add_library(fpdb-main
        src/ExecConfig.cpp include/fpdb/main/ExecConfig.h
        include/fpdb/main/ActorSystemConfig.h
        src/Client.cpp include/fpdb/main/Client.h
        src/Server.cpp include/fpdb/main/Server.h)

target_include_directories(fpdb-main PUBLIC include)

target_link_libraries(fpdb-main PUBLIC fpdb-executor)
target_link_libraries(fpdb-main PUBLIC fpdb-store-server-static)
target_link_libraries(fpdb-main PUBLIC fpdb-util)


# fpdb-main-client
add_executable(fpdb-main-client
        src/ClientMain.cpp)

target_link_libraries(fpdb-main-client PRIVATE fpdb-main)


# fpdb-main-server
add_executable(fpdb-main-server
        src/ServerMain.cpp)

target_link_libraries(fpdb-main-server PRIVATE fpdb-main)

add_dependencies(fpdb-main-server resources)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(fpdb-main-test
        test/Globals.h
        test/TestUtil.cpp test/TestUtil.h
        test/MainTest.cpp
        test/SSBTest.cpp
        test/TPCHTest.cpp
        test/TPCHDistTest.cpp
        test/TPCHFPDBStoreTest.cpp)

target_link_libraries(fpdb-main-test PRIVATE fpdb-main)
target_link_libraries(fpdb-main-test PRIVATE doctest::doctest)

add_dependencies(fpdb-main-test resources)

add_custom_command(
        TARGET fpdb-main-test POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/test-resources
        ${CMAKE_CURRENT_BINARY_DIR}/test-resources)

add_custom_command(
        TARGET fpdb-main-test POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_SOURCE_DIR}/resources
        ${CMAKE_CURRENT_BINARY_DIR}/resources)
