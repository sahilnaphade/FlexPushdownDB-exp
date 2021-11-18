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
        src/message/TupleSetIndexMessage.cpp include/normal/executor/message/TupleSetIndexMessage.h
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
        include/normal/executor/physical/Globals.h
        src/physical/aggregate/AggregatePOp.cpp include/normal/executor/physical/aggregate/AggregatePOp.h
        src/physical/aggregate/AggregationFunction.cpp include/normal/executor/physical/aggregate/AggregationFunction.h
        src/physical/aggregate/AggregationResult.cpp include/normal/executor/physical/aggregate/AggregationResult.h
        src/physical/aggregate/Sum.cpp include/normal/executor/physical/aggregate/Sum.h
        include/normal/executor/physical/aggregate/AggregateBuilder.h
        include/normal/executor/physical/aggregate/AggregateBuilderWrapper.h
        src/physical/cache/CacheLoadPOp.cpp include/normal/executor/physical/cache/CacheLoadPOp.h
        src/physical/cache/CacheHelper.cpp include/normal/executor/physical/cache/CacheHelper.h
        src/physical/collate/CollatePOp.cpp include/normal/executor/physical/collate/CollatePOp.h
        src/physical/collate/CollatePOp2.cpp include/normal/executor/physical/collate/CollatePOp2.h
        src/physical/file/FileScanPOp.cpp include/normal/executor/physical/file/FileScanPOp.h
        src/physical/file/FileScanPOp2.cpp include/normal/executor/physical/file/FileScanPOp2.h
        src/physical/file/FileScanKernel.cpp include/normal/executor/physical/file/FileScanKernel.h
        src/physical/filter/FilterPOp.cpp include/normal/executor/physical/filter/FilterPOp.h
        src/physical/group/GroupPOp.cpp include/normal/executor/physical/group/GroupPOp.h
        include/normal/executor/physical/group/GroupKey2.h
        src/physical/group/GroupKernel2.cpp include/normal/executor/physical/group/GroupKernel2.h
        src/physical/hashjoin/HashJoinBuildPOp.cpp include/normal/executor/physical/hashjoin/HashJoinBuildPOp.h
        src/physical/hashjoin/HashJoinProbePOp.cpp include/normal/executor/physical/hashjoin/HashJoinProbePOp.h
        src/physical/hashjoin/HashJoinBuildKernel2.cpp include/normal/executor/physical/hashjoin/HashJoinBuildKernel2.h
        src/physical/hashjoin/HashJoinProbeKernel2.cpp include/normal/executor/physical/hashjoin/HashJoinProbeKernel2.h
        src/physical/hashjoin/HashJoinPredicate.cpp include/normal/executor/physical/hashjoin/HashJoinPredicate.h
        src/physical/hashjoin/RecordBatchHashJoiner.cpp include/normal/executor/physical/hashjoin/RecordBatchHashJoiner.h)

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
