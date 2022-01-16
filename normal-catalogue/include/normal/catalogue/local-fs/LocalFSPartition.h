//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSPARTITION_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSPARTITION_H

#include <normal/catalogue/Partition.h>
#include <normal/catalogue/Table.h>
#include <normal/caf/CAFUtil.h>
#include <string>
#include <memory>

namespace normal::catalogue::local_fs {

class LocalFSPartition: public Partition {
public:
  LocalFSPartition(const string &path);
  LocalFSPartition() = default;
  LocalFSPartition(const LocalFSPartition&) = default;
  LocalFSPartition& operator=(const LocalFSPartition&) = default;

  [[nodiscard]] const string &getPath() const;

  string toString() override;
  size_t hash() override;
  bool equalTo(shared_ptr<Partition> other) override;
  CatalogueEntryType getCatalogueEntryType() override;

  bool operator==(const LocalFSPartition& other);

private:
  string path_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LocalFSPartition& partition) {
    return f.object(partition).fields(f.field("numBytes", partition.numBytes_),
                                      f.field("zoneMap", partition.zoneMap_),
                                      f.field("path", partition.path_));
  }
};

}

using LocalFSPartitionPtr = std::shared_ptr<normal::catalogue::local_fs::LocalFSPartition>;

CAF_BEGIN_TYPE_ID_BLOCK(LocalFSPartition, normal::caf::CAFUtil::LocalFSPartition_first_custom_type_id)
CAF_ADD_TYPE_ID(LocalFSPartition, (normal::catalogue::local_fs::LocalFSPartition))
CAF_END_TYPE_ID_BLOCK(LocalFSPartition)

namespace caf {
template <>
struct inspector_access<LocalFSPartitionPtr> : variant_inspector_access<LocalFSPartitionPtr> {
  // nop
};
} // namespace caf

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSPARTITION_H
