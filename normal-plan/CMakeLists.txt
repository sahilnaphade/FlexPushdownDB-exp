project(normal-plan VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-plan STATIC
        include/normal/plan/prephysical/PrePOpType.h
        src/calcite/CalcitePlanJsonDeserializer.cpp include/normal/plan/calcite/CalcitePlanJsonDeserializer.h
        src/prephysical/PrePhysicalPlan.cpp include/normal/plan/prephysical/PrePhysicalPlan.h
        src/prephysical/PrePhysicalOp.cpp include/normal/plan/prephysical/PrePhysicalOp.h
        src/prephysical/SortPrePOp.cpp include/normal/plan/prephysical/SortPrePOp.h
        include/normal/plan/prephysical/SortOrder.h
        src/prephysical/SortKey.cpp include/normal/plan/prephysical/SortKey.h
        src/prephysical/LimitSortPrePOp.cpp include/normal/plan/prephysical/LimitSortPrePOp.h
        include/normal/plan/prephysical/AggregatePrePFunctionType.h
        src/prephysical/AggregatePrePFunction.cpp include/normal/plan/prephysical/AggregatePrePFunction.h
        src/prephysical/AggregatePrePOp.cpp include/normal/plan/prephysical/AggregatePrePOp.h
        src/prephysical/GroupPrePOp.cpp include/normal/plan/prephysical/GroupPrePOp.h
        include/normal/plan/prephysical/JoinType.h
        src/prephysical/HashJoinPrePOp.cpp include/normal/plan/prephysical/HashJoinPrePOp.h
        src/prephysical/NestedLoopJoinPrePOp.cpp include/normal/plan/prephysical/NestedLoopJoinPrePOp.h
        src/prephysical/ProjectPrePOp.cpp include/normal/plan/prephysical/ProjectPrePOp.h
        src/prephysical/FilterPrePOp.cpp include/normal/plan/prephysical/FilterPrePOp.h
        src/prephysical/FilterableScanPrePOp.cpp include/normal/plan/prephysical/FilterableScanPrePOp.h
        src/Mode.cpp include/normal/plan/Mode.h
        src/Util.cpp include/normal/plan/Util.h)

target_include_directories(normal-plan PUBLIC include)

target_link_libraries(normal-plan PUBLIC normal-calcite)
target_link_libraries(normal-plan PUBLIC normal-catalogue)
target_link_libraries(normal-plan PUBLIC normal-expression-gandiva)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-plan)
