//
// Created by Yifei Yang on 2/21/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANSERIALIZER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANSERIALIZER_H

#include <fpdb/executor/physical/PhysicalPlan.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreFileScanPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <tl/expected.hpp>
#include <nlohmann/json.hpp>

namespace fpdb::executor::physical {

class PhysicalPlanSerializer {

public:
  PhysicalPlanSerializer(const std::shared_ptr<PhysicalPlan> &physicalPlan);

  tl::expected<std::string, std::string> serialize(bool pretty);

private:
  tl::expected<::nlohmann::json, std::string> serializeDfs(const std::shared_ptr<PhysicalOp> &op);
  tl::expected<::nlohmann::json, std::string> serializeProducers(const std::shared_ptr<PhysicalOp> &op);

  tl::expected<::nlohmann::json, std::string>
  serializeFPDBStoreFileScanPOp(const std::shared_ptr<fpdb_store::FPDBStoreFileScanPOp> &storeFileScanPOp);

  tl::expected<::nlohmann::json, std::string>
  serializeFilterPOp(const std::shared_ptr<filter::FilterPOp> &filterPOp);

  tl::expected<::nlohmann::json, std::string>
  serializeAggregatePOp(const std::shared_ptr<aggregate::AggregatePOp> &aggregatePOp);

  tl::expected<::nlohmann::json, std::string>
  serializeCollatePOp(const std::shared_ptr<collate::CollatePOp> &collatePOp);

  std::unordered_map<std::string, std::shared_ptr<PhysicalOp>> operatorMap_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANSERIALIZER_H
