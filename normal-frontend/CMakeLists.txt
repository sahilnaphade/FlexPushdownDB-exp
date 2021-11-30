project(normal-frontend VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-frontend
        src/ExecConfig.cpp include/normal/frontend/ExecConfig.h
        src/Client.cpp include/normal/frontend/Client.h)

target_include_directories(normal-frontend PUBLIC include)

target_link_libraries(normal-frontend PUBLIC normal-calcite)
target_link_libraries(normal-frontend PUBLIC normal-plan)
target_link_libraries(normal-frontend PUBLIC normal-executor)
target_link_libraries(normal-frontend PUBLIC normal-util)
target_link_libraries(normal-frontend PUBLIC ssl crypto)


add_executable(normal-frontend-main
        src/Main.cpp)

target_link_libraries(normal-frontend-main PRIVATE normal-frontend)
# need to add and link libraries on mac
if (${APPLE})
  target_link_directories(normal-frontend-main PRIVATE /usr/local/opt/openssl/lib)
  target_link_directories(normal-frontend-main PRIVATE /usr/local/opt/llvm@11/lib)
  target_link_directories(normal-frontend-main PRIVATE /usr/local/opt/libdeflate/lib)
  target_link_directories(normal-frontend-main PRIVATE /usr/local/lib)
  # fix "dyld: Library not loaded" on mac
  set(path_to_libs libgvplugin_neato_layout.6.dylib
          libgvplugin_core.6.dylib
          @rpath/libaws-cpp-sdk-core.dylib)
  set(change_libs ${CMAKE_BINARY_DIR}/_deps/graphviz_ep/install/lib/graphviz/libgvplugin_neato_layout.6.dylib
          ${CMAKE_BINARY_DIR}/_deps/graphviz_ep/install/lib/graphviz/libgvplugin_core.6.dylib
          ${CMAKE_BINARY_DIR}/_deps/aws-cpp-sdk_ep/install/lib/libaws-cpp-sdk-core.dylib)
  foreach(path_to_lib change_lib IN ZIP_LISTS path_to_libs change_libs)
    add_custom_command(TARGET normal-frontend-main POST_BUILD
            COMMAND install_name_tool -change ${path_to_lib} ${change_lib} normal-frontend-main)
  endforeach()
endif()


add_executable(TestCalcite src/TestCalcite.cpp)

target_link_libraries(TestCalcite PRIVATE normal-calcite)
target_link_libraries(TestCalcite PRIVATE normal-plan)
target_link_libraries(TestCalcite PRIVATE normal-executor)
target_link_libraries(TestCalcite PRIVATE normal-util)
target_link_libraries(TestCalcite PRIVATE ssl crypto)
# need to add and link libraries on mac
if (${APPLE})
  target_link_directories(TestCalcite PRIVATE /usr/local/opt/openssl/lib)
  target_link_directories(TestCalcite PRIVATE /usr/local/opt/llvm@11/lib)
  target_link_directories(TestCalcite PRIVATE /usr/local/opt/libdeflate/lib)
  target_link_directories(TestCalcite PRIVATE /usr/local/lib)
  # fix "dyld: Library not loaded" on mac
  set(path_to_libs libgvplugin_neato_layout.6.dylib
          libgvplugin_core.6.dylib
          @rpath/libaws-cpp-sdk-core.dylib)
  set(change_libs ${CMAKE_BINARY_DIR}/_deps/graphviz_ep/install/lib/graphviz/libgvplugin_neato_layout.6.dylib
          ${CMAKE_BINARY_DIR}/_deps/graphviz_ep/install/lib/graphviz/libgvplugin_core.6.dylib
          ${CMAKE_BINARY_DIR}/_deps/aws-cpp-sdk_ep/install/lib/libaws-cpp-sdk-core.dylib)
  foreach(path_to_lib change_lib IN ZIP_LISTS path_to_libs change_libs)
    add_custom_command(TARGET TestCalcite POST_BUILD
            COMMAND install_name_tool -change ${path_to_lib} ${change_lib} TestCalcite)
  endforeach()
endif()
