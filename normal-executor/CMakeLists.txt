project(normal-executor VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-executor STATIC
        src/message/Envelope.cpp include/normal/executor/message/Envelope.h
        src/message/Message.cpp include/normal/executor/message/Message.h
        src/message/StartMessage.cpp include/normal/executor/message/StartMessage.h
        src/message/ConnectMessage.cpp include/normal/executor/message/ConnectMessage.h
        src/message/CompleteMessage.cpp include/normal/executor/message/CompleteMessage.h
        src/message/TupleMessage.cpp include/normal/executor/message/TupleMessage.h
        src/message/cache/CacheMetricsMessage.cpp include/normal/executor/message/cache/CacheMetricsMessage.h
        src/message/cache/LoadRequestMessage.cpp include/normal/executor/message/cache/LoadRequestMessage.h
        src/message/cache/LoadResponseMessage.cpp include/normal/executor/message/cache/LoadResponseMessage.h
        src/message/cache/StoreRequestMessage.cpp include/normal/executor/message/cache/StoreRequestMessage.h
        src/message/cache/WeightRequestMessage.cpp include/normal/executor/message/cache/WeightRequestMessage.h
        src/cache/SegmentCacheActor.cpp include/normal/executor/cache/SegmentCacheActor.h
        include/normal/executor/physical/Forward.h
        include/normal/executor/physical/POpRelationshipType.h
        src/physical/POpConnection.cpp include/normal/executor/physical/POpConnection.h
        src/physical/LocalPOpDirectoryEntry.cpp include/normal/executor/physical/LocalPOpDirectoryEntry.h
        src/physical/LocalPOpDirectory.cpp include/normal/executor/physical/LocalPOpDirectory.h
        src/physical/POpDirectory.cpp include/normal/executor/physical/POpDirectory.h
        src/physical/POpDirectoryEntry.cpp include/normal/executor/physical/POpDirectoryEntry.h
        src/physical/POpActor.cpp include/normal/executor/physical/POpActor.h
        src/physical/POpContext.cpp include/normal/executor/physical/POpContext.h
        src/physical/PhysicalOp.cpp include/normal/executor/physical/PhysicalOp.h)

target_include_directories(normal-executor PUBLIC include)

target_link_libraries(normal-executor PUBLIC normal-plan)
target_link_libraries(normal-executor PUBLIC normal-cache)
target_link_libraries(normal-executor PUBLIC normal-expression-gandiva)
target_link_libraries(normal-executor PUBLIC caf::libcaf_core_static)
target_link_libraries(normal-executor PUBLIC caf::libcaf_io_static)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-executor)
