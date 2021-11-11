//
// Created by Yifei Yang on 11/10/21.
//

#include <normal/aws/AWSConfig.h>

namespace normal::aws {

AWSConfig::AWSConfig(S3ClientType s3ClientType,
                     size_t networkLimit) :
  s3ClientType_(s3ClientType),
  networkLimit_(networkLimit) {}

S3ClientType AWSConfig::getS3ClientType() const {
  return s3ClientType_;
}

size_t AWSConfig::getNetworkLimit() const {
  return networkLimit_;
}

}
