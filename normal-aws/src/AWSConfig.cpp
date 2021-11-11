//
// Created by Yifei Yang on 11/10/21.
//

#include "normal/aws/AWSConfig.h"

namespace normal::aws {

AWSConfig::AWSConfig(S3ClientType s3ClientType,
                     const Aws::String &region,
                     size_t networkLimit) :
  s3ClientType_(s3ClientType),
  region_(region),
  networkLimit_(networkLimit) {}

S3ClientType AWSConfig::getS3ClientType() const {
  return s3ClientType_;
}

const Aws::String &AWSConfig::getRegion() const {
  return region_;
}

size_t AWSConfig::getNetworkLimit() const {
  return networkLimit_;
}

}
