project(normal-expression-gandiva VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-expression-gandiva STATIC
        src/Globals.cpp include/normal/expression/gandiva/Globals.h
        src/Expression.cpp include/normal/expression/gandiva/Expression.h
        src/Column.cpp include/normal/expression/gandiva/Column.h
        src/Cast.cpp include/normal/expression/gandiva/Cast.h
        src/Projector.cpp include/normal/expression/gandiva/Projector.h
        src/Add.cpp include/normal/expression/gandiva/Add.h
        src/Divide.cpp include/normal/expression/gandiva/Divide.h
        src/NumericLiteral.cpp include/normal/expression/gandiva/NumericLiteral.h
        src/Multiply.cpp include/normal/expression/gandiva/Multiply.h
        src/Subtract.cpp include/normal/expression/gandiva/Subtract.h
        src/LessThan.cpp include/normal/expression/gandiva/LessThan.h
        src/Filter.cpp include/normal/expression/gandiva/Filter.h
        src/EqualTo.cpp include/normal/expression/gandiva/EqualTo.h
        src/NotEqualTo.cpp include/normal/expression/gandiva/NotEqualTo.h
        src/LessThanOrEqualTo.cpp include/normal/expression/gandiva/LessThanOrEqualTo.h
        src/GreaterThanOrEqualTo.cpp include/normal/expression/gandiva/GreaterThanOrEqualTo.h
        src/GreaterThan.cpp include/normal/expression/gandiva/GreaterThan.h
        src/And.cpp include/normal/expression/gandiva/And.h
        include/normal/expression/gandiva/DateIntervalType.h
        src/DateAdd.cpp include/normal/expression/gandiva/DateAdd.h
        src/Or.cpp include/normal/expression/gandiva/Or.h
        src/StringLiteral.cpp include/normal/expression/gandiva/StringLiteral.h
        src/In.cpp include/normal/expression/gandiva/In.h
        src/Not.cpp include/normal/expression/gandiva/Not.h
        src/If.cpp include/normal/expression/gandiva/If.h
        src/Like.cpp include/normal/expression/gandiva/Like.h
        src/DateExtract.cpp include/normal/expression/gandiva/DateExtract.h
        src/BinaryExpression.cpp include/normal/expression/gandiva/BinaryExpression.h
        src/Canonicalizer.cpp include/normal/expression/gandiva/Canonicalizer.h
        src/Util.cpp include/normal/expression/gandiva/Util.h include/normal/expression/gandiva/ExpressionType.h)

target_include_directories(normal-expression-gandiva PUBLIC include)

target_link_libraries(normal-expression-gandiva PUBLIC gandiva_static)
target_link_libraries(normal-expression-gandiva PUBLIC normal-expression)
target_link_libraries(normal-expression-gandiva PUBLIC normal-util)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-expression-gandiva-test
        test/Globals.h
        test/MainTest.cpp
        test/CastTest.cpp
        test/TestUtil.cpp test/TestUtil.h
        test/BinaryExpressionTest.cpp
        test/GandivaFunctionRegistryTest.cpp
        test/MultiplyTest.cpp test/ThreadTest.cpp)

target_include_directories(normal-expression-gandiva-test PRIVATE include)

target_link_libraries(normal-expression-gandiva-test spdlog::spdlog)
target_link_libraries(normal-expression-gandiva-test doctest::doctest)
target_link_libraries(normal-expression-gandiva-test Backward::Backward)
target_link_libraries(normal-expression-gandiva-test normal-expression-gandiva)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-expression-gandiva)
