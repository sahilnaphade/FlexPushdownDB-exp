
#-----------------------------------------------------------------------------------------------------------------------
# Library
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb_store_server_objlib OBJECT
        src/Module.cpp include/fpdb/store/server/Module.hpp
        src/flight/FlightHandler.cpp include/fpdb/store/server/flight/FlightHandler.hpp
        src/flight/CmdObject.cpp include/fpdb/store/server/flight/CmdObject.hpp
        src/flight/CmdType.cpp include/fpdb/store/server/flight/CmdType.hpp
        src/flight/HeaderMiddleware.cpp include/fpdb/store/server/flight/HeaderMiddleware.hpp
        src/flight/HeaderMiddlewareFactory.cpp include/fpdb/store/server/flight/HeaderMiddlewareFactory.hpp
        src/flight/TicketObject.cpp include/fpdb/store/server/flight/TicketObject.hpp
        src/flight/TicketType.cpp include/fpdb/store/server/flight/TicketType.hpp
        src/flight/FlightSelectObjectContentRequest.cpp include/fpdb/store/server/flight/FlightSelectObjectContentRequest.hpp
        src/flight/FlightInputSerialization.cpp include/fpdb/store/server/flight/FlightInputSerialization.hpp
        src/FileHeaderInfo.cpp include/fpdb/store/server/FileHeaderInfo.hpp
        src/CSVInput.cpp include/fpdb/store/server/CSVInput.hpp
        src/InputSerializationType.cpp include/fpdb/store/server/InputSerializationType.hpp
        src/ParquetInput.cpp include/fpdb/store/server/ParquetInput.hpp
        src/Server.cpp include/fpdb/store/server/Server.hpp include/fpdb/store/server/Global.hpp
        src/SignalHandler.cpp include/fpdb/store/server/SignalHandler.hpp
        src/caf/ActorManager.cpp include/fpdb/store/server/caf/ActorManager.hpp
        src/caf/ServerMeta.cpp include/fpdb/store/server/caf/ServerMeta.hpp
        src/cluster/ClusterActor.cpp include/fpdb/store/server/cluster/ClusterActor.hpp
        src/caf/AbstractActor.cpp include/fpdb/store/server/caf/AbstractActor.hpp
        src/cluster/NodeActor.cpp include/fpdb/store/server/cluster/NodeActor.hpp)

set_target_properties(fpdb_store_server_objlib PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(fpdb_store_server_objlib PROPERTIES OUTPUT_NAME fpdb_store_server)

list(APPEND _PUBLIC_LINK_LIBRARIES expected)
list(APPEND _PUBLIC_LINK_LIBRARIES fpdb-tuple)
list(APPEND _PUBLIC_LINK_LIBRARIES nlohmann_json)

list(APPEND _PRIVATE_LINK_LIBRARIES fmt::fmt)
list(APPEND _PUBLIC_LINK_LIBRARIES caf::libcaf_core_shared)
list(APPEND _PUBLIC_LINK_LIBRARIES caf::libcaf_io_shared)
list(APPEND _PUBLIC_LINK_LIBRARIES caf::libcaf_openssl_shared)

list(APPEND _PUBLIC_SHARED_LINK_LIBRARIES arrow_flight_shared)
list(APPEND _PUBLIC_SHARED_LINK_LIBRARIES fpdb_store_client_shared)

list(APPEND _PUBLIC_STATIC_LINK_LIBRARIES arrow_flight_static)
list(APPEND _PUBLIC_STATIC_LINK_LIBRARIES fpdb_store_client_static)

target_include_directories(fpdb_store_server_objlib PUBLIC include)

target_link_libraries(fpdb_store_server_objlib ${_PUBLIC_LINK_LIBRARIES})
target_link_libraries(fpdb_store_server_objlib ${_PUBLIC_SHARED_LINK_LIBRARIES})
target_link_libraries(fpdb_store_server_objlib ${_PRIVATE_LINK_LIBRARIES})

add_library(fpdb_store_server_static STATIC $<TARGET_OBJECTS:fpdb_store_server_objlib>)
set_target_properties(fpdb_store_server_static PROPERTIES OUTPUT_NAME $<TARGET_PROPERTY:fpdb_store_server_objlib,OUTPUT_NAME>)
target_include_directories(fpdb_store_server_static PUBLIC $<TARGET_PROPERTY:fpdb_store_server_objlib,INCLUDE_DIRECTORIES>)
target_link_libraries(fpdb_store_server_static PUBLIC ${_PUBLIC_LINK_LIBRARIES})
target_link_libraries(fpdb_store_server_static PRIVATE ${_PUBLIC_STATIC_LINK_LIBRARIES})
target_link_libraries(fpdb_store_server_static PRIVATE ${_PRIVATE_LINK_LIBRARIES})

add_library(fpdb_store_server_shared SHARED $<TARGET_OBJECTS:fpdb_store_server_objlib>)
set_target_properties(fpdb_store_server_shared PROPERTIES OUTPUT_NAME $<TARGET_PROPERTY:fpdb_store_server_objlib,OUTPUT_NAME>)
target_include_directories(fpdb_store_server_shared PUBLIC $<TARGET_PROPERTY:fpdb_store_server_objlib,INCLUDE_DIRECTORIES>)
target_link_libraries(fpdb_store_server_shared PUBLIC ${_PUBLIC_LINK_LIBRARIES})
target_link_libraries(fpdb_store_server_shared PRIVATE ${_PUBLIC_SHARED_LINK_LIBRARIES})
target_link_libraries(fpdb_store_server_shared PRIVATE ${_PRIVATE_LINK_LIBRARIES})


#-----------------------------------------------------------------------------------------------------------------------
# Executable
#-----------------------------------------------------------------------------------------------------------------------

add_executable(fpdb_store_server_executable
        executable/Main.cpp executable/Global.hpp)

target_link_libraries(fpdb_store_server_executable fpdb_store_server_static)


#-----------------------------------------------------------------------------------------------------------------------
# Tests
#-----------------------------------------------------------------------------------------------------------------------

add_executable(fpdb_store_server_test
        test/Global.cpp test/Global.hpp test/Main.cpp test/FlightTest.cpp test/DistributedTest.cpp)

target_link_libraries(fpdb_store_server_test fmt::fmt)
target_link_libraries(fpdb_store_server_test spdlog::spdlog)
target_link_libraries(fpdb_store_server_test doctest::doctest)
target_link_libraries(fpdb_store_server_test arrow_flight_shared)
target_link_libraries(fpdb_store_server_test fpdb_store_server_shared)
# need to add and link libraries on mac
if (${APPLE})
    linkForMacExecutable(fpdb_store_server_test)
endif()

add_custom_command(
        TARGET fpdb_store_server_test POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/test-resources
        ${CMAKE_CURRENT_BINARY_DIR})
