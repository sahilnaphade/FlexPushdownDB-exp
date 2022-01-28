project(normal-frontend VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Targets
#-----------------------------------------------------------------------------------------------------------------------

# normal-frontend-client
add_library(normal-frontend
        src/ExecConfig.cpp include/normal/frontend/ExecConfig.h
        include/normal/frontend/ActorSystemConfig.h
        src/CAFInit.cpp include/normal/frontend/CAFInit.h
        src/Client.cpp include/normal/frontend/Client.h
        src/Server.cpp include/normal/frontend/Server.h)

target_include_directories(normal-frontend PUBLIC include)

target_link_libraries(normal-frontend PUBLIC normal-executor)
target_link_libraries(normal-frontend PUBLIC normal-util)
target_link_libraries(normal-frontend PUBLIC ssl crypto)


# normal-frontend-client
add_executable(normal-frontend-client
        src/ClientMain.cpp)

target_link_libraries(normal-frontend-client PRIVATE normal-frontend)
# need to add and link libraries on mac
if (${APPLE})
  linkForMacExecutable(normal-frontend-client)
endif()


# normal-frontend-server
add_executable(normal-frontend-server
        src/ServerMain.cpp)

target_link_libraries(normal-frontend-server PRIVATE normal-frontend)
if (${APPLE})
  linkForMacExecutable(normal-frontend-server)
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

target_link_libraries(normal-frontend-test PRIVATE normal-frontend)
target_link_libraries(normal-frontend-test PRIVATE doctest::doctest)
# need to add and link libraries on mac
if (${APPLE})
  linkForMacExecutable(normal-frontend-test)
endif()
