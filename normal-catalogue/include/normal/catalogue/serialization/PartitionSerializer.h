//
// Created by Yifei Yang on 1/13/22.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_SERIALIZATION_PARTITIONSERIALIZER_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_SERIALIZATION_PARTITIONSERIALIZER_H

#include <normal/catalogue/Partition.h>
#include <normal/catalogue/s3/S3Partition.h>
#include <normal/catalogue/local-fs/LocalFSPartition.h>
#include <normal/caf/CAFUtil.h>

using PartitionPtr = std::shared_ptr<normal::catalogue::Partition>;

CAF_BEGIN_TYPE_ID_BLOCK(Partition, normal::caf::CAFUtil::Partition_first_custom_type_id)
CAF_ADD_TYPE_ID(Partition, (PartitionPtr))
CAF_ADD_TYPE_ID(Partition, (normal::catalogue::s3::S3Partition))
CAF_ADD_TYPE_ID(Partition, (normal::catalogue::local_fs::LocalFSPartition))
CAF_ADD_TYPE_ID(Partition, (std::shared_ptr<normal::catalogue::s3::S3Partition>))
CAF_ADD_TYPE_ID(Partition, (std::shared_ptr<normal::catalogue::local_fs::LocalFSPartition>))
CAF_END_TYPE_ID_BLOCK(Partition)

namespace caf {

template<>
struct variant_inspector_traits<PartitionPtr> {
  using value_type = PartitionPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<normal::catalogue::s3::S3Partition>,
          type_id_v<normal::catalogue::local_fs::LocalFSPartition>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getCatalogueEntryType() == normal::catalogue::S3)
      return 1;
    else if (x->getCatalogueEntryType() == normal::catalogue::LOCAL_FS)
      return 2;
    else
      return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<normal::catalogue::s3::S3Partition &>(*x));
      case 2:
        return f(dynamic_cast<normal::catalogue::local_fs::LocalFSPartition &>(*x));
      default: {
        none_t dummy;
        return f(dummy);
      }
    }
  }

  // Assigns a value to x.
  template<class U>
  static void assign(value_type &x, U value) {
    if constexpr (std::is_same<U, none_t>::value)
      x.reset();
    else
      x = std::make_shared<U>(std::move(value));
  }

  // Create a default-constructed object for `type` and then call the
  // continuation with the temporary object to perform remaining load steps.
  template<class F>
  static bool load(type_id_t type, F continuation) {
    switch (type) {
      default:
        return false;
      case type_id_v<none_t>: {
        none_t dummy;
        continuation(dummy);
        return true;
      }
      case type_id_v<normal::catalogue::s3::S3Partition>: {
        auto tmp = normal::catalogue::s3::S3Partition{};
        continuation(tmp);
        return true;
      }
      case type_id_v<normal::catalogue::local_fs::LocalFSPartition>: {
        auto tmp = normal::catalogue::local_fs::LocalFSPartition{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<PartitionPtr> : variant_inspector_access<PartitionPtr> {
  // nop
};

} // namespace caf


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_SERIALIZATION_PARTITIONSERIALIZER_H
