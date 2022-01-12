project(normal-executor VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-caf STATIC
        src/CAFUtil.cpp include/normal/caf/CAFUtil.h)

target_include_directories(normal-caf PUBLIC include)

target_link_libraries(normal-caf PUBLIC caf::libcaf_core_shared)
target_link_libraries(normal-caf PUBLIC caf::libcaf_io_shared)