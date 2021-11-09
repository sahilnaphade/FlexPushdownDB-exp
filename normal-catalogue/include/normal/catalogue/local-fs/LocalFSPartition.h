//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H
#define NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H

#include <normal/catalogue/Partition.h>
#include <string>
#include <memory>

namespace normal::catalogue::local_fs {

class LocalFSPartition: public Partition {
public:
  explicit LocalFSPartition(std::string path);

  [[nodiscard]] const std::string &getPath() const;

  std::string toString() override;
  size_t hash() override;

  bool equalTo(std::shared_ptr<Partition> other) override;

  bool operator==(const LocalFSPartition& other);

private:
  std::string path_;
};

}

#endif //NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H
