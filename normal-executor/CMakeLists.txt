project(normal-executor VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-executor STATIC
#        messages
        src/message/Envelope.cpp include/normal/executor/message/Envelope.h
        src/message/Message.cpp include/normal/executor/message/Message.h
        src/message/StartMessage.cpp include/normal/executor/message/StartMessage.h
        src/message/ConnectMessage.cpp include/normal/executor/message/ConnectMessage.h
        src/message/CompleteMessage.cpp include/normal/executor/message/CompleteMessage.h
        src/message/TupleMessage.cpp include/normal/executor/message/TupleMessage.h
        src/message/ScanMessage.cpp include/normal/executor/message/ScanMessage.h
        src/message/cache/CacheMetricsMessage.cpp include/normal/executor/message/cache/CacheMetricsMessage.h
        src/message/cache/LoadRequestMessage.cpp include/normal/executor/message/cache/LoadRequestMessage.h
        src/message/cache/LoadResponseMessage.cpp include/normal/executor/message/cache/LoadResponseMessage.h
        src/message/cache/StoreRequestMessage.cpp include/normal/executor/message/cache/StoreRequestMessage.h
        src/message/cache/WeightRequestMessage.cpp include/normal/executor/message/cache/WeightRequestMessage.h
#        base physical operator, operator and segment cache actors and required sources
        src/cache/SegmentCacheActor.cpp include/normal/executor/cache/SegmentCacheActor.h
        include/normal/executor/physical/Forward.h
        include/normal/executor/physical/POpRelationshipType.h
        src/physical/POpConnection.cpp include/normal/executor/physical/POpConnection.h
        src/physical/LocalPOpDirectoryEntry.cpp include/normal/executor/physical/LocalPOpDirectoryEntry.h
        src/physical/LocalPOpDirectory.cpp include/normal/executor/physical/LocalPOpDirectory.h
        src/physical/POpDirectory.cpp include/normal/executor/physical/POpDirectory.h
        src/physical/POpDirectoryEntry.cpp include/normal/executor/physical/POpDirectoryEntry.h
        src/physical/POpActor.cpp include/normal/executor/physical/POpActor.h
        include/normal/executor/physical/POpActor2.h
        src/physical/POpContext.cpp include/normal/executor/physical/POpContext.h
        src/physical/PhysicalOp.cpp include/normal/executor/physical/PhysicalOp.h
#        physical operators
        src/physical/aggregate/Aggregate.cpp include/normal/executor/physical/aggregate/Aggregate.h
        src/physical/aggregate/AggregationFunction.cpp include/normal/executor/physical/aggregate/AggregationFunction.h
        src/physical/aggregate/AggregationResult.cpp include/normal/executor/physical/aggregate/AggregationResult.h
        src/physical/aggregate/Sum.cpp include/normal/executor/physical/aggregate/Sum.h
        include/normal/executor/physical/aggregate/AggregateBuilder.h
        include/normal/executor/physical/aggregate/AggregateBuilderWrapper.h
        src/physical/cache/CacheLoad.cpp include/normal/executor/physical/cache/CacheLoad.h
        src/physical/cache/CacheHelper.cpp include/normal/executor/physical/cache/CacheHelper.h)

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
