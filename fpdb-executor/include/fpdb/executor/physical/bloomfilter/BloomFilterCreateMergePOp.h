//
// Created by Yifei Yang on 3/18/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEMERGEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEMERGEPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilter.h>

namespace fpdb::executor::physical::bloomfilter {

/**
 * Op to merge bloom filters from parallel BloomFilterCreatePOp to a single one, and then send it to BloomFilterPOp
 */
class BloomFilterCreateMergePOp: public PhysicalOp {

public:
  explicit BloomFilterCreateMergePOp(const std::string &name,
                                     const std::vector<std::string> &projectColumnNames,
                                     int nodeId);
  BloomFilterCreateMergePOp() = default;
  BloomFilterCreateMergePOp(const BloomFilterCreateMergePOp&) = default;
  BloomFilterCreateMergePOp& operator=(const BloomFilterCreateMergePOp&) = default;
  ~BloomFilterCreateMergePOp() = default;

  void onReceive(const Envelope &envelope) override;
  void clear() override;
  std::string getTypeString() const override;

private:
  void onStart();
  void onBloomFilter(const BloomFilterMessage &msg);
  void onComplete(const CompleteMessage &);

  std::optional<std::shared_ptr<BloomFilter>> mergedBloomFilter_;
  int nBloomFilterReceived_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEMERGEPOP_H
