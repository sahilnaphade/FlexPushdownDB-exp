project(normal-expression VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-expression STATIC
        include/normal/expression/Globals.h
        src/Expression.cpp include/normal/expression/Expression.h
        src/Projector.cpp include/normal/expression/Projector.h
        src/Filter.cpp include/normal/expression/Filter.h)

target_include_directories(normal-expression PUBLIC include)

target_link_libraries(normal-expression PUBLIC normal-tuple)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-expression)
