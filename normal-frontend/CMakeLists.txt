project(normal-frontend VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Targets: normal-frontend-client
#-----------------------------------------------------------------------------------------------------------------------

# normal-frontend-client
add_library(normal-frontend-client
        src/ExecConfig.cpp include/normal/frontend/ExecConfig.h
        include/normal/frontend/ClientActorSystemConfig.h
        src/Client.cpp include/normal/frontend/Client.h)

target_include_directories(normal-frontend-client PUBLIC include)

target_link_libraries(normal-frontend-client PUBLIC normal-executor)
target_link_libraries(normal-frontend-client PUBLIC normal-util)
target_link_libraries(normal-frontend-client PUBLIC ssl crypto)


# normal-frontend-client-main
add_executable(normal-frontend-client-main
        src/Main.cpp)

target_link_libraries(normal-frontend-client-main PRIVATE normal-frontend-client)
# need to add and link libraries on mac
if (${APPLE})
  linkForMacExecutable(normal-frontend-client-main)
endif()


#-----------------------------------------------------------------------------------------------------------------------
# Targets: normal-frontend-server
#-----------------------------------------------------------------------------------------------------------------------

# normal-frontend-server
add_library(normal-frontend-server
        include/normal/frontend/ServerActorSystemConfig.h
        src/Server.cpp include/normal/frontend/Server.h)

target_include_directories(normal-frontend-server PUBLIC include)
target_link_libraries(normal-frontend-server PUBLIC normal-util)
target_link_libraries(normal-frontend-server PUBLIC caf::libcaf_core_shared)
target_link_libraries(normal-frontend-server PUBLIC caf::libcaf_io_shared)
target_link_libraries(normal-frontend-server PUBLIC ssl crypto)


# normal-frontend-server-main
add_executable(normal-frontend-server-main
        src/ServerMain.cpp)

target_link_libraries(normal-frontend-server-main PRIVATE normal-frontend-server)
if (${APPLE})
  target_link_directories(normal-frontend-server-main PRIVATE /usr/local/opt/openssl/lib)
endif()


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-frontend-test
        test/Globals.h
        test/TestUtil.cpp test/TestUtil.h
        test/MainTest.cpp
        test/SSBTest.cpp
        test/TPCHTest.cpp)

target_link_libraries(normal-frontend-test PRIVATE normal-calcite)
target_link_libraries(normal-frontend-test PRIVATE normal-plan)
target_link_libraries(normal-frontend-test PRIVATE normal-executor)
target_link_libraries(normal-frontend-test PRIVATE normal-util)
target_link_libraries(normal-frontend-test PRIVATE ssl crypto)
target_link_libraries(normal-frontend-test PRIVATE doctest::doctest)
# need to add and link libraries on mac
if (${APPLE})
  linkForMacExecutable(normal-frontend-test)
endif()
