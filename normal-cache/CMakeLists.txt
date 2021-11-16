project(normal-cache VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-cache STATIC
        src/Globals.cpp include/normal/cache/Globals.h
        src/Util.cpp include/normal/cache/Util.h
        src/SegmentCache.cpp include/normal/cache/SegmentCache.h
        src/SegmentKey.cpp include/normal/cache/SegmentKey.h
        src/SegmentData.cpp include/normal/cache/SegmentData.h
        src/SegmentRange.cpp include/normal/cache/SegmentRange.h
        src/SegmentMetadata.cpp include/normal/cache/SegmentMetadata.h
        src/policy/CachingPolicy.cpp include/normal/cache/policy/CachingPolicy.h
        src/policy/LRUCachingPolicy.cpp include/normal/cache/policy/LRUCachingPolicy.h
        src/policy/LFUCachingPolicy.cpp include/normal/cache/policy/LFUCachingPolicy.h
        src/policy/LFUSCachingPolicy.cpp include/normal/cache/policy/LFUSCachingPolicy.h
        src/policy/WLFUCachingPolicy.cpp include/normal/cache/policy/WLFUCachingPolicy.h
        src/policy/BeladyCachingPolicyHelper.cpp include/normal/cache/policy/BeladyCachingPolicyHelper.h
        src/policy/BeladyCachingPolicy.cpp include/normal/cache/policy/BeladyCachingPolicy.h)

target_include_directories(normal-cache PUBLIC include)

target_link_libraries(normal-cache PUBLIC normal-plan)
target_link_libraries(normal-cache PUBLIC normal-tuple)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-cache-test
        test/Globals.h
        test/MainTest.cpp
        test/CacheTest.cpp)


target_include_directories(normal-cache-test PRIVATE include)

target_link_libraries(normal-cache-test doctest::doctest)
target_link_libraries(normal-cache-test Backward::Backward)
target_link_libraries(normal-cache-test normal-cache)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-cache)
