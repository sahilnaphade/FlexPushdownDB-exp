//
// Created by Yifei Yang on 2/21/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESUPERPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESUPERPOP_H

#include <fpdb/executor/physical/PhysicalPlan.h>
#include <tl/expected.hpp>

namespace fpdb::executor::physical::fpdb_store {

/**
 * Denote a sub-plan to be pushed to store, consists a group of physical operators (e.g. scan->filter->aggregate)
 */
class FPDBStoreSuperPOp : public PhysicalOp {

public:
  FPDBStoreSuperPOp(const std::string &name,
                    const std::vector<std::string> &projectColumnNames,
                    int nodeId,
                    const std::shared_ptr<PhysicalPlan> &subPlan,
                    const std::string &host,
                    int port);
  FPDBStoreSuperPOp() = default;
  FPDBStoreSuperPOp(const FPDBStoreSuperPOp&) = default;
  FPDBStoreSuperPOp& operator=(const FPDBStoreSuperPOp&) = default;
  ~FPDBStoreSuperPOp() = default;

  void onReceive(const Envelope &envelope) override;
  void clear() override;
  std::string getTypeString() const override;

  /**
   * Add op as the last op (except collate) in the subPlan
   * @param lastOp
   */
  void addAsLastOp(std::shared_ptr<PhysicalOp> &lastOp);

private:
  void onStart();
  void onBloomFilter(const BloomFilterMessage &msg);
  void onComplete(const CompleteMessage &);

  bool readyToProcess();
  void processAtStore();
  tl::expected<std::string, std::string> serialize(bool pretty);

  std::shared_ptr<PhysicalPlan> subPlan_;
  std::string host_;
  int port_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, FPDBStoreSuperPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("subPlan", op.subPlan_),
                               f.field("host", op.host_),
                               f.field("port", op.port_));
  }

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESUPERPOP_H
