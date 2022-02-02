project(normal-aws VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# AWS
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-aws
        include/normal/aws/S3ClientType.h
        src/AWSClient.cpp include/normal/aws/AWSClient.h
        src/AWSConfig.cpp include/normal/aws/AWSConfig.h
        src/ProfileAWSCredentialsProviderChain.cpp include/normal/aws/ProfileAWSCredentialsProviderChain.h
        src/S3Util.cpp include/normal/aws/S3Util.h)

target_include_directories(normal-aws PUBLIC include)

target_link_libraries(normal-aws PUBLIC normal-util)
target_link_libraries(normal-aws PUBLIC spdlog::spdlog)
target_link_libraries(normal-aws PUBLIC aws-cpp-sdk-core)
target_link_libraries(normal-aws PUBLIC aws-cpp-sdk-s3)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-aws)
