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
  static tl::expected<std::shared_ptr<PhysicalPlan>, std::string> deserialize(const std::string &planString,
                                                                              const std::string &storeRootPath);

private:
  PhysicalPlanDeserializer(const std::string &planString,
                           const std::string &storeRootPath);

  /**
   * Impl of deserialization
   * @return
   */
  tl::expected<std::shared_ptr<PhysicalPlan>, std::string> deserialize();

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeDfs(::nlohmann::json jObj,
                                                                        bool isRoot = false);

  tl::expected<std::vector<std::shared_ptr<PhysicalOp>>, std::string> deserializeProducers(::nlohmann::json jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeFPDBStoreFileScanPOp(::nlohmann::json jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeFilterPOp(::nlohmann::json jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeProjectPOp(::nlohmann::json jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeAggregatePOp(::nlohmann::json jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeBloomFilterUsePOp(::nlohmann::json jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeCollatePOp(::nlohmann::json jObj);

  std::string planString_;
  std::string storeRootPath_;

  std::unordered_map<std::string, std::shared_ptr<PhysicalOp>> physicalOps_;
  std::string rootPOpName_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANDESERIALIZER_H
