//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSPARTITION_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSPARTITION_H

#include <normal/catalogue/Partition.h>
#include <string>
#include <memory>

namespace normal::catalogue::local_fs {

class LocalFSPartition: public Partition {
public:
  LocalFSPartition(const string &path);

  [[nodiscard]] const string &getPath() const;

  string toString() override;
  size_t hash() override;

  bool equalTo(shared_ptr<Partition> other) override;

  bool operator==(const LocalFSPartition& other);

private:
  string path_;
};

}

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSPARTITION_H
