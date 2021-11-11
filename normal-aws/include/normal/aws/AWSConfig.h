//
// Created by Yifei Yang on 11/10/21.
//

#ifndef NORMAL_NORMAL_AWS_INCLUDE_NORMAL_AWS_AWSCONFIG_H
#define NORMAL_NORMAL_AWS_INCLUDE_NORMAL_AWS_AWSCONFIG_H

#include <normal/aws/S3ClientType.h>
#include <aws/core/Aws.h>

namespace normal::aws {

class AWSConfig {
public:
  AWSConfig(S3ClientType s3ClientType, const Aws::String &region, size_t networkLimit);

  S3ClientType getS3ClientType() const;
  const Aws::String &getRegion() const;
  size_t getNetworkLimit() const;

private:
  S3ClientType s3ClientType_;
  Aws::String region_;
  size_t networkLimit_;
};

}


#endif //NORMAL_NORMAL_AWS_INCLUDE_NORMAL_AWS_AWSCONFIG_H
