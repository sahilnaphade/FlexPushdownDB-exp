//
// Created by Yifei Yang on 6/26/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FPDBSTOREEXECUTION_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FPDBSTOREEXECUTION_H

#include <fpdb/executor/Execution.h>

namespace fpdb::executor {

class FPDBStoreExecution: public Execution {

public:
  FPDBStoreExecution(long queryId,
                     const std::shared_ptr<::caf::actor_system> &actorSystem,
                     const std::shared_ptr<PhysicalPlan> &physicalPlan);
  ~FPDBStoreExecution() override = default;

  const std::unordered_map<std::string, std::shared_ptr<TupleSet>> &getTupleSets() const;
  const std::unordered_map<std::string, std::vector<int64_t>> &getBitmaps() const;

private:
  void join() override;

  // for shuffled tupleSets at storage side (consumer name -> tupleSet)
  std::unordered_map<std::string, std::shared_ptr<TupleSet>> tupleSets_;

  // for bitmap constructed at storage side (producer name -> bitmap)
  std::unordered_map<std::string, std::vector<int64_t>> bitmaps_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FPDBSTOREEXECUTION_H
