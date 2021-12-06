project(normal-frontend VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

# normal-frontend
add_library(normal-frontend
        src/ExecConfig.cpp include/normal/frontend/ExecConfig.h
        src/Client.cpp include/normal/frontend/Client.h)

target_include_directories(normal-frontend PUBLIC include)

target_link_libraries(normal-frontend PUBLIC normal-plan)
target_link_libraries(normal-frontend PUBLIC normal-executor)
target_link_libraries(normal-frontend PUBLIC normal-util)
target_link_libraries(normal-frontend PUBLIC ssl crypto)


# normal-frontend-main
add_executable(normal-frontend-main
        src/Main.cpp)

target_link_libraries(normal-frontend-main PRIVATE normal-frontend)
# need to add and link libraries on mac
if (${APPLE})
  linkForMacExecutable(normal-frontend-main)
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
