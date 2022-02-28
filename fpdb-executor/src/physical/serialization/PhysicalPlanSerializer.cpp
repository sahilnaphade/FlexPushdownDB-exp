//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/serialization/PhysicalPlanSerializer.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>

using namespace fpdb::tuple;
using json = nlohmann::json;

namespace fpdb::executor::physical {

PhysicalPlanSerializer::PhysicalPlanSerializer(const std::shared_ptr<PhysicalPlan> &physicalPlan) {
  for (const auto &op: physicalPlan->getPhysicalOps()) {
    operatorMap_.emplace(op->name(), op);
  }
}

tl::expected<std::string, std::string> PhysicalPlanSerializer::serialize(bool pretty) {
  auto rootIt = operatorMap_.find("Collate");
  if (rootIt == operatorMap_.end()) {
    return tl::make_unexpected("Collate (root) operator not found in StoreSuperPOp");
  }
  auto expRootJObj = serializeDfs(rootIt->second);
  if (!expRootJObj.has_value()) {
    return expRootJObj.error();
  }
  return (*expRootJObj).dump(pretty ? 2 : -1);
}

tl::expected<json, std::string> PhysicalPlanSerializer::serializeDfs(const std::shared_ptr<PhysicalOp> &op) {
  switch (op->getType()) {
    case POpType::STORE_FILE_SCAN:
      return serializeStoreFileScanPOp(std::static_pointer_cast<store::StoreFileScanPOp>(op));
    case POpType::FILTER:
      return serializeFilterPOp(std::static_pointer_cast<filter::FilterPOp>(op));
    case POpType::AGGREGATE:
      return serializeAggregatePOp(std::static_pointer_cast<aggregate::AggregatePOp>(op));
    case POpType::COLLATE:
      return serializeCollatePOp(std::static_pointer_cast<collate::CollatePOp>(op));
    default:
      return tl::make_unexpected(fmt::format("Unsupported physical operator type at store: '{}'",
                                             op->getTypeString()));
  }
}

tl::expected<json, std::string> PhysicalPlanSerializer::serializeProducers(const std::shared_ptr<PhysicalOp> &op) {
  std::vector<json> producerJArr;

  for (const auto &name: op->producers()) {
    auto producerIt = operatorMap_.find(name);
    if (producerIt == operatorMap_.end()) {
      return tl::make_unexpected(fmt::format("Operator '{}' not found in StoreSuperPOp", name));
    }
    auto expProducerJObj = serializeDfs(producerIt->second);
    if (!expProducerJObj.has_value()) {
      return expProducerJObj.error();
    }
    producerJArr.emplace_back(*expProducerJObj);
  }

  return producerJArr;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeStoreFileScanPOp(const std::shared_ptr<store::StoreFileScanPOp> &storeFileScanPOp) {
  json jObj;
  auto kernel = storeFileScanPOp->getKernel();

  // serialize self
  jObj.emplace("type", storeFileScanPOp->getTypeString());
  jObj.emplace("name", storeFileScanPOp->name());
  jObj.emplace("projectColumnNames", storeFileScanPOp->getProjectColumnNames());
  jObj.emplace("bucket", storeFileScanPOp->getBucket());
  jObj.emplace("object", storeFileScanPOp->getObject());
  jObj.emplace("format", kernel->getFormat()->toJson());
  jObj.emplace("schema", ArrowSerializer::schema_to_bytes(kernel->getSchema()));

  auto optByteRange = kernel->getByteRange();
  if (optByteRange.has_value()) {
    json byteRangeJObj;
    byteRangeJObj.emplace("startOffset", optByteRange->first);
    byteRangeJObj.emplace("finishOffset", optByteRange->second);
    jObj.emplace("byteRange", byteRangeJObj);
  }

  // serialize producers
  auto expProducersJObj = serializeProducers(storeFileScanPOp);
  if (!expProducersJObj.has_value()) {
    return expProducersJObj.error();
  }
  jObj.emplace("inputs", *expProducersJObj);

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeFilterPOp(const std::shared_ptr<filter::FilterPOp> &filterPOp) {
  json jObj;

  // serialize self
  jObj.emplace("type", filterPOp->getTypeString());
  jObj.emplace("name", filterPOp->name());
  jObj.emplace("projectColumnNames", filterPOp->getProjectColumnNames());
  jObj.emplace("predicate", filterPOp->getPredicate()->toJson());

  // serialize producers
  auto expProducersJObj = serializeProducers(filterPOp);
  if (!expProducersJObj.has_value()) {
    return expProducersJObj.error();
  }
  jObj.emplace("inputs", *expProducersJObj);

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeAggregatePOp(const std::shared_ptr<aggregate::AggregatePOp> &aggregatePOp) {
  json jObj;

  // serialize self
  jObj.emplace("type", aggregatePOp->getTypeString());
  jObj.emplace("name", aggregatePOp->name());
  jObj.emplace("projectColumnNames", aggregatePOp->getProjectColumnNames());
  vector<json> functionsJArr;
  for (const auto &function: aggregatePOp->getFunctions()) {
    functionsJArr.emplace_back(function->toJson());
  }
  jObj.emplace("functions", functionsJArr);

  // serialize producers
  auto expProducersJObj = serializeProducers(aggregatePOp);
  if (!expProducersJObj.has_value()) {
    return expProducersJObj.error();
  }
  jObj.emplace("inputs", *expProducersJObj);

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeCollatePOp(const std::shared_ptr<collate::CollatePOp> &collatePOp) {
  json jObj;

  // serialize producers
  jObj.emplace("type", collatePOp->getTypeString());
  jObj.emplace("name", collatePOp->name());
  jObj.emplace("projectColumnNames", collatePOp->getProjectColumnNames());
  auto expProducersJObj = serializeProducers(collatePOp);
  if (!expProducersJObj.has_value()) {
    return expProducersJObj.error();
  }
  jObj.emplace("inputs", *expProducersJObj);

  return jObj;
}

}
