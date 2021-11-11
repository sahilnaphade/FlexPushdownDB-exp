//
// Created by Yifei Yang on 11/10/21.
//

#ifndef NORMAL_NORMAL_AWS_INCLUDE_NORMAL_AWS_AWSCONFIG_H
#define NORMAL_NORMAL_AWS_INCLUDE_NORMAL_AWS_AWSCONFIG_H

#include <normal/aws/S3ClientType.h>
#include <utility>

namespace normal::aws {

class AWSConfig {
public:
  AWSConfig(S3ClientType s3ClientType,
            size_t networkLimit);

  S3ClientType getS3ClientType() const;
  size_t getNetworkLimit() const;

private:
  S3ClientType s3ClientType_;
  size_t networkLimit_;
};

}


#endif //NORMAL_NORMAL_AWS_INCLUDE_NORMAL_AWS_AWSCONFIG_H
