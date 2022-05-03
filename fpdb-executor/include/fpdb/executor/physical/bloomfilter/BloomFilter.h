//
// Created by Yifei Yang on 3/17/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTER_H

#include <fpdb/executor/physical/bloomfilter/UniversalHashFunction.h>
#include <fpdb/caf/CAFUtil.h>
#include <tl/expected.hpp>
#include <vector>

namespace fpdb::executor::physical::bloomfilter {

class BloomFilter {

public:
  inline static constexpr double DefaultDesiredFalsePositiveRate = 0.01;

  BloomFilter(int64_t capacity, double falsePositiveRate);
  static std::shared_ptr<BloomFilter> make(int64_t capacity, double falsePositiveRate);

  /**
   * Used when reconstructing at store during pushdown
   */
  BloomFilter(int64_t capacity,
              double falsePositiveRate,
              bool valid,
              int64_t numHashFunctions,
              int64_t numBits,
              const std::vector<std::shared_ptr<UniversalHashFunction>> &hashFunctions,
              const std::vector<int64_t> &bitArray);
  static std::shared_ptr<BloomFilter> make(int64_t capacity,
                                           double falsePositiveRate,
                                           bool valid,
                                           int64_t numHashFunctions,
                                           int64_t numBits,
                                           const std::vector<std::shared_ptr<UniversalHashFunction>> &hashFunctions,
                                           const std::vector<int64_t> &bitArray);

  BloomFilter() = default;
  BloomFilter(const BloomFilter&) = default;
  BloomFilter& operator=(const BloomFilter&) = default;
  virtual ~BloomFilter() = default;

  void init();
  void add(int64_t key);
  bool contains(int64_t key);

  const std::vector<int64_t> getBitArray() const;
  void setBitArray(const std::vector<int64_t> &bitArray);
  bool valid() const;

  /**
   * Merge another bloom filter into this
   * @param other
   */
  tl::expected<void, std::string> merge(const std::shared_ptr<BloomFilter> &other);

  ::nlohmann::json toJson() const;
  static tl::expected<std::shared_ptr<BloomFilter>, std::string> fromJson(const nlohmann::json &jObj);

private:
  int64_t capacity_;
  double falsePositiveRate_;

  bool valid_;    // whether this bloom filter will be used after runtime checking (i.e. false if input is too large)

  int64_t numHashFunctions_;
  int64_t numBits_;

  std::vector<std::shared_ptr<UniversalHashFunction>> hashFunctions_;
  std::vector<int64_t> bitArray_;

  int64_t calculateNumHashFunctions() const;
  int64_t calculateNumBits() const;

  std::vector<std::shared_ptr<UniversalHashFunction>> makeHashFunctions();
  std::vector<int64_t> makeBitArray() const;

  std::vector<int64_t> hashes(int64_t key);

  /**
   * Conversion formulas below
   *
   * n Capacity
   * p False positive rate
   * k Number of hash functions
   * m Number of bits
   */

  /**
   *
   * @param p False positive rate
   * @return k Number of hash functions
   */
  static int64_t k_from_p(double p);

  /**
   *
   * @param n Capacity
   * @param p False positive rate
   * @return m Number of bits
   */
  static int64_t m_from_np(int64_t n, double p);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BloomFilter& bf) {
    return f.object(bf).fields(f.field("capacity", bf.capacity_),
                               f.field("falsePositiveRate", bf.falsePositiveRate_));
  }
};

}

using BloomFilterPtr = std::shared_ptr<fpdb::executor::physical::bloomfilter::BloomFilter>;

CAF_BEGIN_TYPE_ID_BLOCK(BloomFilter, fpdb::caf::CAFUtil::BloomFilter_first_custom_type_id)
CAF_ADD_TYPE_ID(BloomFilter, (fpdb::executor::physical::bloomfilter::BloomFilter))
CAF_END_TYPE_ID_BLOCK(BloomFilter)

namespace caf {
template <>
struct inspector_access<BloomFilterPtr> : variant_inspector_access<BloomFilterPtr> {
  // nop
};
} // namespace caf


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTER_H
