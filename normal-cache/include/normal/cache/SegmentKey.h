//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTKEY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTKEY_H

#include <normal/cache/SegmentRange.h>
#include <normal/cache/SegmentMetadata.h>
#include <normal/catalogue/Partition.h>
#include <normal/catalogue/serialization/PartitionSerializer.h>
#include <normal/caf/CAFUtil.h>
#include <memory>

using namespace normal::catalogue;

namespace normal::cache {

class SegmentKey {

public:
  SegmentKey(std::shared_ptr<Partition> Partition,
             std::string columnName,
             SegmentRange Range);

  SegmentKey(std::shared_ptr<Partition> Partition,
             std::string columnName,
             SegmentRange Range,
             std::shared_ptr<SegmentMetadata> metadata);

  SegmentKey() = default;
  SegmentKey(const SegmentKey&) = default;
  SegmentKey& operator=(const SegmentKey&) = default;

  static std::shared_ptr<SegmentKey> make(const std::shared_ptr<Partition> &Partition,
                                          const std::string& columnName,
                                          SegmentRange Range);

  static std::shared_ptr<SegmentKey> make(const std::shared_ptr<Partition> &Partition,
                                          const std::string& columnName,
                                          SegmentRange Range,
                                          const std::shared_ptr<SegmentMetadata>& metadata);

  [[nodiscard]] const std::shared_ptr<Partition> &getPartition() const;
  [[nodiscard]] const std::string &getColumnName() const;
  [[maybe_unused]] [[nodiscard]] const SegmentRange &getRange() const;
  [[nodiscard]] const std::shared_ptr<SegmentMetadata> &getMetadata() const;

  std::string toString();

  bool operator==(const SegmentKey& other) const;
  bool operator!=(const SegmentKey& other) const;

  size_t hash();

  void setMetadata(const std::shared_ptr<SegmentMetadata> &metadata);

private:
  std::shared_ptr<Partition> partition_;
  std::string columnName_;
  SegmentRange range_;
  std::shared_ptr<SegmentMetadata> metadata_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SegmentKey& key) {
    return f.object(key).fields(f.field("partition", key.partition_),
                                f.field("columnName", key.columnName_),
                                f.field("range", key.range_),
                                f.field("metadata", key.metadata_));
  }
};

struct SegmentKeyPointerHash {
  inline size_t operator()(const std::shared_ptr<SegmentKey> &key) const {
	return key->hash();
  }
};

struct SegmentKeyPointerPredicate {
  inline bool operator()(const std::shared_ptr<SegmentKey>& lhs, const std::shared_ptr<SegmentKey>& rhs) const {
	return *lhs == *rhs;
  }
};

}

using SegmentKeyPtr = std::shared_ptr<normal::cache::SegmentKey>;

CAF_BEGIN_TYPE_ID_BLOCK(SegmentKey, normal::caf::CAFUtil::SegmentKey_first_custom_type_id)
CAF_ADD_TYPE_ID(SegmentKey, (normal::cache::SegmentKey))
CAF_END_TYPE_ID_BLOCK(SegmentKey)

namespace caf {
template <>
struct inspector_access<SegmentKeyPtr> : variant_inspector_access<SegmentKeyPtr> {
  // nop
};
} // namespace caf

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTKEY_H
