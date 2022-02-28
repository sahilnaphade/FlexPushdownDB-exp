//
// Created by Yifei Yang on 2/21/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_STORE_STORESUPERPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_STORE_STORESUPERPOP_H

#include <fpdb/executor/physical/PhysicalPlan.h>

namespace fpdb::executor::physical::store {

/**
 * Denote a sub-plan to be pushed to store, consists a group of physical operators (e.g. scan->filter->aggregate)
 */
class StoreSuperPOp : public PhysicalOp {

public:
  StoreSuperPOp(const std::string &name,
                const std::vector<std::string> &projectColumnNames,
                int nodeId,
                const std::shared_ptr<PhysicalPlan> &subPlan);
  StoreSuperPOp() = default;
  StoreSuperPOp(const StoreSuperPOp&) = default;
  StoreSuperPOp& operator=(const StoreSuperPOp&) = default;
  ~StoreSuperPOp() = default;

  void onReceive(const Envelope &message) override;
  void clear() override;
  std::string getTypeString() const override;

  std::string serialize(bool pretty);

private:
  std::shared_ptr<PhysicalPlan> subPlan_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, StoreSuperPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("subPlan", op.subPlan_));
  }

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_STORE_STORESUPERPOP_H
