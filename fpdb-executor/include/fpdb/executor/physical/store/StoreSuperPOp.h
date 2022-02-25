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

  void onReceive(const Envelope &message) override;
  void clear() override;
  std::string getTypeString() const override;

  std::string serialize(bool pretty);

public:
  std::shared_ptr<PhysicalPlan> subPlan_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_STORE_STORESUPERPOP_H
