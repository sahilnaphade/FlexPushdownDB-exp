//
// Created by Yifei Yang on 3/16/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateKernel.h>
#include <fpdb/executor/message/BloomFilterMessage.h>

namespace fpdb::executor::physical::bloomfilter {

class BloomFilterCreatePOp: public PhysicalOp {

public:
  explicit BloomFilterCreatePOp(const std::string &name,
                                const std::vector<std::string> &projectColumnNames,
                                int nodeId,
                                const std::vector<std::string> &bloomFilterColumnNames);
  BloomFilterCreatePOp() = default;
  BloomFilterCreatePOp(const BloomFilterCreatePOp&) = default;
  BloomFilterCreatePOp& operator=(const BloomFilterCreatePOp&) = default;
  ~BloomFilterCreatePOp() = default;

  void onReceive(const Envelope &envelope) override;
  void clear() override;
  std::string getTypeString() const override;

private:
  void onStart();
  void onTupleSet(const TupleSetMessage &msg);
  void onBloomFilter(const BloomFilterMessage &msg);
  void onComplete(const CompleteMessage &);

  BloomFilterCreateKernel kernel_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BloomFilterCreatePOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("kernel", op.kernel_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEPOP_H
