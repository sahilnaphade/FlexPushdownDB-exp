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
#        base physical operators, operator actors, segment cache actors and required sources
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
        include/normal/executor/physical/POpType.h
#        physical operators
        include/normal/executor/physical/Globals.h
        src/physical/sort/SortPOp.cpp include/normal/executor/physical/sort/SortPOp.h
        src/physical/limitsort/LimitSortPOp.cpp include/normal/executor/physical/limitsort/LimitSortPOp.h
        src/physical/aggregate/AggregatePOp.cpp include/normal/executor/physical/aggregate/AggregatePOp.h
        src/physical/aggregate/AggregateResult.cpp include/normal/executor/physical/aggregate/AggregateResult.h
        include/normal/executor/physical/aggregate/function/AggregateFunctionType.h
        src/physical/aggregate/function/AggregateFunction.cpp include/normal/executor/physical/aggregate/function/AggregateFunction.h
        src/physical/aggregate/function/Sum.cpp include/normal/executor/physical/aggregate/function/Sum.h
        src/physical/aggregate/function/Count.cpp include/normal/executor/physical/aggregate/function/Count.h
        src/physical/aggregate/function/MinMax.cpp include/normal/executor/physical/aggregate/function/MinMax.h
        src/physical/cache/CacheLoadPOp.cpp include/normal/executor/physical/cache/CacheLoadPOp.h
        src/physical/cache/CacheHelper.cpp include/normal/executor/physical/cache/CacheHelper.h
        src/physical/collate/CollatePOp.cpp include/normal/executor/physical/collate/CollatePOp.h
        src/physical/collate/CollatePOp2.cpp include/normal/executor/physical/collate/CollatePOp2.h
        src/physical/file/FileScanPOp.cpp include/normal/executor/physical/file/FileScanPOp.h
        src/physical/file/FileScanPOp2.cpp include/normal/executor/physical/file/FileScanPOp2.h
        src/physical/file/FileScanKernel.cpp include/normal/executor/physical/file/FileScanKernel.h
        src/physical/filter/FilterPOp.cpp include/normal/executor/physical/filter/FilterPOp.h
        src/physical/group/GroupPOp.cpp include/normal/executor/physical/group/GroupPOp.h
        src/physical/group/GroupKernel2.cpp include/normal/executor/physical/group/GroupKernel2.h
        src/physical/join/hashjoin/HashJoinBuildPOp.cpp include/normal/executor/physical/join/hashjoin/HashJoinBuildPOp.h
        src/physical/join/hashjoin/HashJoinProbePOp.cpp include/normal/executor/physical/join/hashjoin/HashJoinProbePOp.h
        src/physical/join/hashjoin/HashJoinBuildKernel.cpp include/normal/executor/physical/join/hashjoin/HashJoinBuildKernel.h
        src/physical/join/hashjoin/HashJoinProbeAbstractKernel.cpp include/normal/executor/physical/join/hashjoin/HashJoinProbeAbstractKernel.h
        src/physical/join/hashjoin/HashJoinProbeKernel.cpp include/normal/executor/physical/join/hashjoin/HashJoinProbeKernel.h
        src/physical/join/hashjoin/HashSemiJoinProbeKernel.cpp include/normal/executor/physical/join/hashjoin/HashSemiJoinProbeKernel.h
        src/physical/join/hashjoin/HashJoinPredicate.cpp include/normal/executor/physical/join/hashjoin/HashJoinPredicate.h
        src/physical/join/hashjoin/RecordBatchHashJoiner.cpp include/normal/executor/physical/join/hashjoin/RecordBatchHashJoiner.h
        src/physical/join/hashjoin/RecordBatchHashSemiJoiner.cpp include/normal/executor/physical/join/hashjoin/RecordBatchHashSemiJoiner.h
        src/physical/join/nestedloopjoin/NestedLoopJoinPOp.cpp include/normal/executor/physical/join/nestedloopjoin/NestedLoopJoinPOp.h
        src/physical/join/nestedloopjoin/NestedLoopJoinKernel.cpp include/normal/executor/physical/join/nestedloopjoin/NestedLoopJoinKernel.h
        src/physical/join/nestedloopjoin/RecordBatchNestedLoopJoiner.cpp include/normal/executor/physical/join/nestedloopjoin/RecordBatchNestedLoopJoiner.h
        src/physical/join/OuterJoinHelper.cpp include/normal/executor/physical/join/OuterJoinHelper.h
        src/physical/merge/MergePOp.cpp include/normal/executor/physical/merge/MergePOp.h
        src/physical/merge/MergeKernel.cpp include/normal/executor/physical/merge/MergeKernel.h
        src/physical/project/ProjectPOp.cpp include/normal/executor/physical/project/ProjectPOp.h
        src/physical/shuffle/ShufflePOp.cpp include/normal/executor/physical/shuffle/ShufflePOp.h
        src/physical/shuffle/ShuffleKernel2.cpp include/normal/executor/physical/shuffle/ShuffleKernel2.h
        src/physical/shuffle/RecordBatchShuffler2.cpp include/normal/executor/physical/shuffle/RecordBatchShuffler2.h
        src/physical/split/SplitPOp.cpp include/normal/executor/physical/split/SplitPOp.h
        src/physical/s3/S3GetPOp.cpp include/normal/executor/physical/s3/S3GetPOp.h
        src/physical/s3/S3SelectPOp.cpp include/normal/executor/physical/s3/S3SelectPOp.h
        src/physical/s3/S3SelectScanAbstractPOp.cpp include/normal/executor/physical/s3/S3SelectScanAbstractPOp.h
        src/physical/s3/S3CSVParser.cpp include/normal/executor/physical/s3/S3CSVParser.h
        src/physical/s3/S3SelectCSVParseOptions.cpp include/normal/executor/physical/s3/S3SelectCSVParseOptions.h
        src/physical/s3/S3SelectScanKernel.cpp include/normal/executor/physical/s3/S3SelectScanKernel.h
#        physical plan
        src/physical/PhysicalPlan.cpp include/normal/executor/physical/PhysicalPlan.h
        src/physical/transform/PrePToPTransformer.cpp include/normal/executor/physical/transform/PrePToPTransformer.h
        src/physical/transform/PrePToS3PTransformer.cpp include/normal/executor/physical/transform/PrePToS3PTransformer.h
        src/physical/prune/PartitionPruner.cpp include/normal/executor/physical/prune/PartitionPruner.h
#        CAF serializer
        include/normal/executor/serialization/MessageSerializer.h
        include/normal/executor/serialization/POpSerializer.h
        include/normal/executor/serialization/AggregateFunctionSerializer.h
        include/normal/executor/serialization/HashJoinProbeAbstractKernelSerializer.h
#        executor
        src/Executor.cpp include/normal/executor/Executor.h
        src/Execution.cpp include/normal/executor/Execution.h)

target_include_directories(normal-executor PUBLIC include)

target_link_libraries(normal-executor PUBLIC normal-plan)
target_link_libraries(normal-executor PUBLIC normal-cache)
target_link_libraries(normal-executor PUBLIC normal-caf)
target_link_libraries(normal-executor PUBLIC graphviz_gvc_shared)
target_link_libraries(normal-executor PUBLIC graphviz_dot_layout_shared)
target_link_libraries(normal-executor PUBLIC graphviz_neato_layout_shared)


#-----------------------------------------------------------------------------------------------------------------------
# Test Data
#-----------------------------------------------------------------------------------------------------------------------

file(COPY test/data/ DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/test/data/)

#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-executor)
