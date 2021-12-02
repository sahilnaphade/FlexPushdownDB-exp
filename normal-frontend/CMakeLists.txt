project(normal-frontend VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

#-------------------------------------------------------------------------------------------------------------
# Function for additional commands for mac os
#-----------------------------------------------------------------------------------------------------------------------

function(linkForMacExecutable target)
  # missing linking on mac
  target_link_directories(${target} PRIVATE /usr/local/opt/openssl/lib)
  target_link_directories(${target} PRIVATE /usr/local/opt/llvm/lib)
  target_link_directories(${target} PRIVATE /usr/local/opt/libdeflate/lib)

  # remove -lrt from link.txt which mac does not have or need
  set(linkFile ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/${target}.dir/link.txt)
  message(${linkFile})
  add_custom_command(TARGET ${target} PRE_LINK
          COMMAND sed -i '' 's/ -lrt//g' ${linkFile})

  # fix "dyld: Library not loaded" on mac
  set(path_to_libs libgvplugin_neato_layout.6.dylib
          libgvplugin_core.6.dylib
          @rpath/libaws-cpp-sdk-core.dylib)
  set(change_libs ${CMAKE_BINARY_DIR}/_deps/graphviz_ep/install/lib/graphviz/libgvplugin_neato_layout.6.dylib
          ${CMAKE_BINARY_DIR}/_deps/graphviz_ep/install/lib/graphviz/libgvplugin_core.6.dylib
          ${CMAKE_BINARY_DIR}/_deps/aws-cpp-sdk_ep/install/lib/libaws-cpp-sdk-core.dylib)
  foreach(path_to_lib change_lib IN ZIP_LISTS path_to_libs change_libs)
    add_custom_command(TARGET ${target} POST_BUILD
            COMMAND install_name_tool -change ${path_to_lib} ${change_lib} ${target})
  endforeach()
endfunction()


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

# normal-frontend
add_library(normal-frontend
        src/ExecConfig.cpp include/normal/frontend/ExecConfig.h
        src/Client.cpp include/normal/frontend/Client.h)

target_include_directories(normal-frontend PUBLIC include)

target_link_libraries(normal-frontend PUBLIC normal-calcite)
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
