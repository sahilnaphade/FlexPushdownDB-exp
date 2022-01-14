//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTRANGE_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTRANGE_H

#include <string>

namespace normal::cache {

class SegmentRange {

public:
  SegmentRange(unsigned long StartOffset, unsigned long FinishOffset);
  SegmentRange() = default;
  SegmentRange(const SegmentRange&) = default;
  SegmentRange& operator=(const SegmentRange&) = default;

  static SegmentRange make(unsigned long StartOffset, unsigned long FinishOffset);

  std::string toString();

  bool operator==(const SegmentRange& other) const;

  [[nodiscard]] size_t hash() const;

private:
  unsigned long startOffset_;
  unsigned long finishOffset_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SegmentRange& range) {
    return f.object(range).fields(f.field("startOffset_", range.startOffset_),
                                  f.field("finishOffset", range.finishOffset_));
  }
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTRANGE_H
