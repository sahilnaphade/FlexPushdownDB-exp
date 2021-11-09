//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H
#define NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H

#include <normal/catalogue/Partition.h>
#include <string>
#include <memory>

namespace normal::catalogue::s3 {

class S3Partition: public Partition {
public:
  explicit S3Partition(std::string bucket, std::string object);
  explicit S3Partition(std::string bucket, std::string object, long numBytes);

  [[nodiscard]] const std::string &getBucket() const;
  [[nodiscard]] const std::string &getObject() const;

  std::string toString() override;
  size_t hash() override;

  bool equalTo(std::shared_ptr<Partition> other) override;

  bool operator==(const S3Partition& other);

private:
  std::string s3Bucket_;
  std::string s3Object_;
};

}

#endif //NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H
