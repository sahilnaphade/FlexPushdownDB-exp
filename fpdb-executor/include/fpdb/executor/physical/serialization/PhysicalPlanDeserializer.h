//
// Created by Yifei Yang on 2/21/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANDESERIALIZER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANDESERIALIZER_H

#include <fpdb/executor/physical/PhysicalPlan.h>
#include <tl/expected.hpp>

namespace fpdb::executor::physical {

class PhysicalPlanDeserializer {

public:
  PhysicalPlanDeserializer(const std::string &planString,
                           const std::string &storeRootPath);
  
  tl::expected<std::shared_ptr<PhysicalPlan>, std::string> deserialize();

private:
  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeDfs(::nlohmann::json jObj);

  tl::expected<std::vector<std::shared_ptr<PhysicalOp>>, std::string> deserializeProducers(::nlohmann::json jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeFPDBStoreFileScanPOp(::nlohmann::json jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeFilterPOp(::nlohmann::json jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeProjectPOp(::nlohmann::json jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeAggregatePOp(::nlohmann::json jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeCollatePOp(::nlohmann::json jObj);

  std::string planString_;
  std::string storeRootPath_;

  std::vector<std::shared_ptr<PhysicalOp>> physicalOps_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANDESERIALIZER_H
