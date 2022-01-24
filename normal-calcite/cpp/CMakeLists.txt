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
        COMMAND thrift_executable --gen cpp -r -o ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../thrift/CalciteServer.thrift)

add_library(calciteserver_thrift_cpp STATIC
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
        COMMAND thrift_executable --gen java -r -out ${CMAKE_CURRENT_SOURCE_DIR}/../java/main/java ${CMAKE_CURRENT_SOURCE_DIR}/../thrift/CalciteServer.thrift
        COMMAND mvn install -f ${CMAKE_SOURCE_DIR}/normal-calcite/java/pom.xml -DskipTests)


#-----------------------------------------------------------------------------------------------------------------------
# Calcite
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-calcite STATIC
        src/CalciteClient.cpp include/normal/calcite/CalciteClient.h
        src/CalciteConfig.cpp include/normal/calcite/CalciteConfig.h)

target_include_directories(normal-calcite PUBLIC include)

target_link_libraries(normal-calcite PUBLIC calciteserver_thrift_cpp)
target_link_libraries(normal-calcite PUBLIC thrift_static)
target_link_libraries(normal-calcite PUBLIC boost)
target_link_libraries(normal-calcite PRIVATE normal-util)
add_dependencies(normal-calcite calciteserver_thrift_java)
