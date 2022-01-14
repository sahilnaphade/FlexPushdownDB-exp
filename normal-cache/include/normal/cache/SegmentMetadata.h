//
// Created by Yifei Yang on 7/30/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H

#include <normal/caf/CAFUtil.h>
#include <memory>

namespace normal::cache {

class SegmentMetadata {

public:
  SegmentMetadata(size_t size);
  SegmentMetadata(const SegmentMetadata &m);
  SegmentMetadata() = default;
  SegmentMetadata& operator=(const SegmentMetadata&) = default;

  static std::shared_ptr<SegmentMetadata> make(size_t size);

  [[nodiscard]] size_t size() const;
  [[nodiscard]] int hitNum() const;
  [[nodiscard]] double perSizeFreq() const;
  [[nodiscard]] double value() const;
  [[nodiscard]] bool valid() const;

  void setSize(size_t size);
  void incHitNum();
  void incHitNum(size_t size);
  void addValue(double value);
  void invalidate();

private:
  size_t size_;
  int hitNum_;
  double perSizeFreq_;
  double value_;
  bool valid_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SegmentMetadata& m) {
    return f.object(m).fields(f.field("size", m.size_),
                              f.field("hitNum", m.hitNum_),
                              f.field("perSizeFreq", m.perSizeFreq_),
                              f.field("value", m.value_),
                              f.field("valid", m.valid_));
  }
};

}

CAF_BEGIN_TYPE_ID_BLOCK(SegmentMetadata, normal::caf::CAFUtil::SegmentMetadata_first_custom_type_id)
CAF_ADD_TYPE_ID(SegmentMetadata, (normal::cache::SegmentMetadata))
CAF_END_TYPE_ID_BLOCK(SegmentMetadata)

using SegmentMetadataPtr = std::shared_ptr<normal::cache::SegmentMetadata>;

namespace caf {
template <>
struct inspector_access<SegmentMetadataPtr> : variant_inspector_access<SegmentMetadataPtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H
