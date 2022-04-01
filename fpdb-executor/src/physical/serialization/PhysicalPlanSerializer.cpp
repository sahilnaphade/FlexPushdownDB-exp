//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/serialization/PhysicalPlanSerializer.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>

using namespace fpdb::tuple;
using json = nlohmann::json;

namespace fpdb::executor::physical {

PhysicalPlanSerializer::PhysicalPlanSerializer(const std::shared_ptr<PhysicalPlan> &physicalPlan,
                                               bool pretty):
  physicalPlan_(physicalPlan),
  pretty_(pretty){}

tl::expected<std::string, std::string>
PhysicalPlanSerializer::serialize(const std::shared_ptr<PhysicalPlan> &physicalPlan,
                                  bool pretty) {
  PhysicalPlanSerializer serializer(physicalPlan, pretty);
  return serializer.serialize();
}

tl::expected<std::string, std::string> PhysicalPlanSerializer::serialize() {
  // get root op
  auto expRootOp = physicalPlan_->getRootPOp();
  if (!expRootOp.has_value()) {
    return tl::make_unexpected(expRootOp.error());
  }
  auto rootOp = *expRootOp;

  // serialize in DFS
  auto expRootJObj = serializeDfs(rootOp);
  if (!expRootJObj.has_value()) {
    return tl::make_unexpected(expRootJObj.error());
  }
  return (*expRootJObj).dump(pretty_ ? 2 : -1);
}

tl::expected<json, std::string> PhysicalPlanSerializer::serializeDfs(const std::shared_ptr<PhysicalOp> &op) {
  switch (op->getType()) {
    case POpType::FPDB_STORE_FILE_SCAN:
      return serializeFPDBStoreFileScanPOp(std::static_pointer_cast<fpdb_store::FPDBStoreFileScanPOp>(op));
    case POpType::FILTER:
      return serializeFilterPOp(std::static_pointer_cast<filter::FilterPOp>(op));
    case POpType::PROJECT:
      return serializeProjectPOp(std::static_pointer_cast<project::ProjectPOp>(op));
    case POpType::AGGREGATE:
      return serializeAggregatePOp(std::static_pointer_cast<aggregate::AggregatePOp>(op));
    case POpType::BLOOM_FILTER_USE:
      return serializeBloomFilterUsePOp(std::static_pointer_cast<bloomfilter::BloomFilterUsePOp>(op));
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
    auto expProducer = physicalPlan_->getPhysicalOp(name);
    if (!expProducer.has_value()) {
      return tl::make_unexpected(expProducer.error());
    }
    auto expProducerJObj = serializeDfs(*expProducer);
    if (!expProducerJObj.has_value()) {
      return tl::make_unexpected(expProducerJObj.error());
    }
    producerJArr.emplace_back(*expProducerJObj);
  }

  return producerJArr;
}

tl::expected<::nlohmann::json, std::string> PhysicalPlanSerializer::serializeFPDBStoreFileScanPOp(
        const std::shared_ptr<fpdb_store::FPDBStoreFileScanPOp> &storeFileScanPOp) {
  auto kernel = storeFileScanPOp->getKernel();

  // serialize self
  auto jObj = serializePOpCommon(storeFileScanPOp);
  jObj.emplace("bucket", storeFileScanPOp->getBucket());
  jObj.emplace("object", storeFileScanPOp->getObject());
  jObj.emplace("format", kernel->getFormat()->toJson());
  jObj.emplace("schema", ArrowSerializer::schema_to_bytes(kernel->getSchema()));
  jObj.emplace("fileSize", kernel->getFileSize());

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
    return tl::make_unexpected(expProducersJObj.error());
  }
  jObj.emplace("inputs", *expProducersJObj);

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeFilterPOp(const std::shared_ptr<filter::FilterPOp> &filterPOp) {
  // serialize self
  auto jObj = serializePOpCommon(filterPOp);
  jObj.emplace("predicate", filterPOp->getPredicate()->toJson());

  // serialize producers
  auto expProducersJObj = serializeProducers(filterPOp);
  if (!expProducersJObj.has_value()) {
    return tl::make_unexpected(expProducersJObj.error());
  }
  jObj.emplace("inputs", *expProducersJObj);

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeProjectPOp(const std::shared_ptr<project::ProjectPOp> &projectPOp) {
  // serialize self
  auto jObj = serializePOpCommon(projectPOp);

  vector<json> exprsJArr;
  for (const auto &expr: projectPOp->getExprs()) {
    exprsJArr.emplace_back(expr->toJson());
  }
  jObj.emplace("exprs", exprsJArr);

  jObj.emplace("exprNames", projectPOp->getExprNames());
  jObj.emplace("projectColumnNamePairs", projectPOp->getProjectColumnNamePairs());

  // serialize producers
  auto expProducersJObj = serializeProducers(projectPOp);
  if (!expProducersJObj.has_value()) {
    return tl::make_unexpected(expProducersJObj.error());
  }
  jObj.emplace("inputs", *expProducersJObj);

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeAggregatePOp(const std::shared_ptr<aggregate::AggregatePOp> &aggregatePOp) {
  // serialize self
  auto jObj = serializePOpCommon(aggregatePOp);
  vector<json> functionsJArr;
  for (const auto &function: aggregatePOp->getFunctions()) {
    functionsJArr.emplace_back(function->toJson());
  }
  jObj.emplace("functions", functionsJArr);

  // serialize producers
  auto expProducersJObj = serializeProducers(aggregatePOp);
  if (!expProducersJObj.has_value()) {
    return tl::make_unexpected(expProducersJObj.error());
  }
  jObj.emplace("inputs", *expProducersJObj);

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeBloomFilterUsePOp(const std::shared_ptr<bloomfilter::BloomFilterUsePOp> &bloomFilterUsePOp) {
  // serialize self
  auto jObj = serializePOpCommon(bloomFilterUsePOp);
  jObj.emplace("bloomFilterColumnNames", bloomFilterUsePOp->getBloomFilterColumnNames());

  auto bloomFilter = bloomFilterUsePOp->getBloomFilter();
  if (!bloomFilter.has_value()) {
    return tl::make_unexpected("Bloom filter not set in BloomFilterUsePOp");
  }
  jObj.emplace("bloomFilter", (*bloomFilter)->toJson());

  // serialize producers
  auto expProducersJObj = serializeProducers(bloomFilterUsePOp);
  if (!expProducersJObj.has_value()) {
    return tl::make_unexpected(expProducersJObj.error());
  }
  jObj.emplace("inputs", *expProducersJObj);

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeCollatePOp(const std::shared_ptr<collate::CollatePOp> &collatePOp) {
  // serialize self
  auto jObj = serializePOpCommon(collatePOp);

  // serialize producers
  auto expProducersJObj = serializeProducers(collatePOp);
  if (!expProducersJObj.has_value()) {
    return tl::make_unexpected(expProducersJObj.error());
  }
  jObj.emplace("inputs", *expProducersJObj);

  return jObj;
}

::nlohmann::json PhysicalPlanSerializer::serializePOpCommon(const std::shared_ptr<PhysicalOp> &op) {
  json jObj;
  jObj.emplace("type", op->getTypeString());
  jObj.emplace("name", op->name());
  jObj.emplace("projectColumnNames", op->getProjectColumnNames());
  jObj.emplace("isSeparated", op->isSeparated());
  return jObj;
}

}
