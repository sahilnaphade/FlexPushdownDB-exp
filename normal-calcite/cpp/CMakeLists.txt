project(normal-connector VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

#-----------------------------------------------------------------------------------------------------------------------
# Calcite server thrift - cpp
#-----------------------------------------------------------------------------------------------------------------------

add_custom_command(
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/../thrift/CalciteServer.thrift
        OUTPUT
          ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer.cpp
          ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer_types.cpp
          ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer_server.skeleton.cpp
        COMMAND thrift --gen cpp -r -o ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../thrift/CalciteServer.thrift)

add_library(calciteserver_thrift_cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer_types.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer_server.skeleton.cpp)

target_link_libraries(calciteserver_thrift_cpp PRIVATE thrift_static)
target_link_libraries(calciteserver_thrift_cpp PRIVATE boost)


#-----------------------------------------------------------------------------------------------------------------------
# Calcite server thrift - java
#-----------------------------------------------------------------------------------------------------------------------

add_custom_target(
        calciteserver_thrift_java ALL
        COMMAND thrift --gen java -r -out ${CMAKE_CURRENT_SOURCE_DIR}/../java/main/java ${CMAKE_CURRENT_SOURCE_DIR}/../thrift/CalciteServer.thrift)


#-----------------------------------------------------------------------------------------------------------------------
# Calcite
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-calcite
        src/Calcite.cpp include/normal/calcite/Calcite.h)

target_include_directories(normal-calcite PUBLIC include)

target_link_libraries(normal-calcite PRIVATE calciteserver_thrift_cpp)
target_link_libraries(normal-calcite PRIVATE thrift_static)
target_link_libraries(normal-calcite PRIVATE boost)

add_dependencies(normal-calcite calciteserver_thrift_java)
