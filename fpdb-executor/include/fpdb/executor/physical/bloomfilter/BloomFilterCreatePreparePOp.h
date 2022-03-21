//
// Created by Yifei Yang on 3/18/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEPREPAREPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEPREPAREPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilter.h>

namespace fpdb::executor::physical::bloomfilter {

/**
 * Op to prepare for parallel BloomFilterCreatePOp,
 * e.g. make sure all parallel BloomFilterCreatePOp have same features like capacity, hash functions...
 */
class BloomFilterCreatePreparePOp: public PhysicalOp {

public:
  explicit BloomFilterCreatePreparePOp(const std::string &name,
                                       const std::vector<std::string> &projectColumnNames,
                                       int nodeId,
                                       double desiredFalsePositiveRate = BloomFilter::DefaultDesiredFalsePositiveRate);
  BloomFilterCreatePreparePOp() = default;
  BloomFilterCreatePreparePOp(const BloomFilterCreatePreparePOp&) = default;
  BloomFilterCreatePreparePOp& operator=(const BloomFilterCreatePreparePOp&) = default;
  ~BloomFilterCreatePreparePOp() = default;

  void onReceive(const Envelope &envelope) override;
  void clear() override;
  std::string getTypeString() const override;

private:
  void onStart();
  void onTupleSetSize(const TupleSetSizeMessage &msg);
  void onComplete(const CompleteMessage &);

  double desiredFalsePositiveRate_;
  int64_t capacity_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BloomFilterCreatePreparePOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("desiredFalsePositiveRate", op.desiredFalsePositiveRate_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEPREPAREPOP_H
