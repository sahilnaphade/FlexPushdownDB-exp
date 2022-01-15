project(normal-util VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Util
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-util
        include/normal/util/Traits.h
        src/Util.cpp include/normal/util/Util.h)

target_include_directories(normal-util PUBLIC include)

target_link_libraries(normal-util PUBLIC LLVM-filesystem)
target_link_libraries(normal-util PUBLIC fmt::fmt)
target_link_libraries(normal-util PUBLIC spdlog::spdlog)
target_link_libraries(normal-util PUBLIC expected)
target_link_libraries(normal-util PUBLIC boost_callable_traits)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-util-test
        test/Globals.h
        test/MainTest.cpp
        test/TraitsTest.cpp
        test/UtilTest.cpp)

target_link_libraries(normal-util-test normal-util)
target_link_libraries(normal-util-test fmt::fmt)
target_link_libraries(normal-util-test spdlog::spdlog)
target_link_libraries(normal-util-test doctest::doctest)
# need to add and link libraries on mac
if (${APPLE})
  linkForMacExecutable(normal-util-test)
endif()


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-util)
