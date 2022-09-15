//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/serialization/PhysicalPlanDeserializer.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreFileScanPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/shuffle/ShufflePOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>

using json = ::nlohmann::json;

namespace fpdb::executor::physical {

PhysicalPlanDeserializer::PhysicalPlanDeserializer(const std::string &planString,
                                                   const std::string &storeRootPath):
  planString_(planString),
  storeRootPath_(storeRootPath) {}

tl::expected<std::shared_ptr<PhysicalPlan>, std::string>
PhysicalPlanDeserializer::deserialize(const std::string &planString,
                                      const std::string &storeRootPath) {
  PhysicalPlanDeserializer deserializer(planString, storeRootPath);
  return deserializer.deserialize();
}

tl::expected<std::shared_ptr<PhysicalPlan>, std::string> PhysicalPlanDeserializer::deserialize() {
  try {
    // parse
    auto jObj = json::parse(planString_);
    if (!jObj.is_object()) {
      return tl::make_unexpected(fmt::format("Cannot parse physical plan JSON '{}", planString_));
    }

    // deserialize in DFS order
    auto deserializeRes = deserializeDfs(jObj, true);
    if (!deserializeRes.has_value()) {
      return tl::make_unexpected(deserializeRes.error());
    }

    return std::make_shared<PhysicalPlan>(physicalOps_, rootPOpName_);
  } catch(nlohmann::json::parse_error& ex) {
    return tl::make_unexpected(ex.what());
  }
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeDfs(const ::nlohmann::json &jObj, bool isRoot) {
  if (!jObj.contains("type")) {
    return tl::make_unexpected(fmt::format("Type not specified in physical operator JSON '{}'", jObj));
  }
  if (isRoot) {
    if (!jObj.contains("name")) {
      return tl::make_unexpected(fmt::format("Name not specified in physical operator JSON '{}'", jObj));
    }
    rootPOpName_ = jObj["name"].get<std::string>();
  }

  auto type = jObj["type"].get<std::string>();
  if (type == "FPDBStoreFileScanPOp") {
    return deserializeFPDBStoreFileScanPOp(jObj);
  } else if (type == "FilterPOp") {
    return deserializeFilterPOp(jObj);
  } else if (type == "ProjectPOp") {
    return deserializeProjectPOp(jObj);
  } else if (type == "AggregatePOp") {
    return deserializeAggregatePOp(jObj);
  } else if (type == "ShufflePOp") {
    return deserializeShufflePOp(jObj);
  } else if (type == "CollatePOp") {
    return deserializeCollatePOp(jObj);
  } else {
    return tl::make_unexpected(fmt::format("Unsupported physical operator type at store: '{}'", type));
  }
}

tl::expected<std::vector<std::shared_ptr<PhysicalOp>>, std::string>
PhysicalPlanDeserializer::deserializeProducers(const ::nlohmann::json &jObj) {
  if (!jObj.contains("inputs")) {
    return tl::make_unexpected(fmt::format("Inputs not specified in physical operator JSON '{}'", jObj));
  }

  std::vector<std::shared_ptr<PhysicalOp>> producers;
  auto producersJArr = jObj["inputs"].get<std::vector<json>>();
  for (const auto &producerJObj: producersJArr) {
    auto expProducer = deserializeDfs(producerJObj);
    if (!expProducer.has_value()) {
      return tl::make_unexpected(expProducer.error());
    }
    producers.emplace_back(*expProducer);
  }
  return producers;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeFPDBStoreFileScanPOp(const ::nlohmann::json &jObj) {
  // deserialize self
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto isSeparated = std::get<2>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<3>(commonTuple);

  if (!jObj.contains("bucket")) {
    return tl::make_unexpected(fmt::format("Bucket not specified in FileScanPOp JSON '{}'", jObj));
  }
  auto bucket = jObj["bucket"].get<std::string>();

  if (!jObj.contains("object")) {
    return tl::make_unexpected(fmt::format("Object not specified in FileScanPOp JSON '{}'", jObj));
  }
  auto object = jObj["object"].get<std::string>();

  if (!jObj.contains("format")) {
    return tl::make_unexpected(fmt::format("Format not specified in FileScanPOp JSON '{}'", jObj));
  }
  auto expFormat = FileFormat::fromJson(jObj["format"]);
  if (!expFormat) {
    return tl::make_unexpected(expFormat.error());
  }
  auto format = *expFormat;

  if (!jObj.contains("schema")) {
    return tl::make_unexpected(fmt::format("Schema not specified in FileScanPOp JSON '{}'", jObj));
  }
  auto schema = ArrowSerializer::bytes_to_schema(jObj["schema"].get<std::vector<uint8_t>>());

  if (!jObj.contains("fileSize")) {
    return tl::make_unexpected(fmt::format("FileSize not specified in FileScanPOp JSON '{}'", jObj));
  }
  auto fileSize = jObj["fileSize"].get<int64_t>();

  std::optional<std::pair<int64_t, int64_t>> byteRange = std::nullopt;
  if (jObj.contains("byteRange")) {
    auto byteRangeJObj = jObj["byteRange"];
    if (!byteRangeJObj.contains("startOffset")) {
      return tl::make_unexpected(fmt::format("StartOffset not specified in byteRange JSON '{}'", byteRangeJObj));
    }
    auto startOffset = byteRangeJObj["startOffset"].get<int64_t>();
    if (!byteRangeJObj.contains("finishOffset")) {
      return tl::make_unexpected(fmt::format("FinishOffset not specified in byteRange JSON '{}'", byteRangeJObj));
    }
    auto finishOffset = byteRangeJObj["finishOffset"].get<int64_t>();
    byteRange = {startOffset, finishOffset};
  }

  std::shared_ptr<PhysicalOp> storeFileScanPOp = std::make_shared<fpdb_store::FPDBStoreFileScanPOp>(name,
                                                                                                    projectColumnNames,
                                                                                                    0,
                                                                                                    storeRootPath_,
                                                                                                    bucket,
                                                                                                    object,
                                                                                                    format,
                                                                                                    schema,
                                                                                                    fileSize,
                                                                                                    byteRange);
  storeFileScanPOp->setSeparated(isSeparated);
  storeFileScanPOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);
  physicalOps_.emplace(storeFileScanPOp->name(), storeFileScanPOp);
  
  // deserialize producers
  auto expProducers = deserializeProducers(jObj);
  if (!expProducers.has_value()) {
    return tl::make_unexpected(expProducers.error());
  }

  // connect
  PrePToPTransformerUtil::connectManyToOne(*expProducers, storeFileScanPOp);

  return storeFileScanPOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeFilterPOp(const ::nlohmann::json &jObj) {
  // deserialize self
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto isSeparated = std::get<2>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<3>(commonTuple);

  if (!jObj.contains("predicate")) {
    return tl::make_unexpected(fmt::format("Predicate not specified in FilterPOp JSON '{}'", jObj));
  }
  auto expPredicate = fpdb::expression::gandiva::Expression::fromJson(jObj["predicate"]);
  if (!expPredicate.has_value()) {
    return tl::make_unexpected(expPredicate.error());
  }
  auto predicate = *expPredicate;

  std::shared_ptr<PhysicalOp> filterPOp = std::make_shared<filter::FilterPOp>(name,
                                                                              projectColumnNames,
                                                                              0,
                                                                              predicate);
  filterPOp->setSeparated(isSeparated);
  filterPOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);
  physicalOps_.emplace(filterPOp->name(), filterPOp);

  if (jObj.contains("bitmapWrapper")) {
    auto expBitmapWrapper = FPDBStoreFilterBitmapWrapper::fromJson(jObj["bitmapWrapper"]);
    if (!expBitmapWrapper.has_value()) {
      return tl::make_unexpected(expBitmapWrapper.error());
    }
    std::static_pointer_cast<filter::FilterPOp>(filterPOp)->setBitmapWrapper(*expBitmapWrapper);
  }

  // deserialize producers
  auto expProducers = deserializeProducers(jObj);
  if (!expProducers.has_value()) {
    return tl::make_unexpected(expProducers.error());
  }

  // connect
  PrePToPTransformerUtil::connectManyToOne(*expProducers, filterPOp);

  return filterPOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeProjectPOp(const ::nlohmann::json &jObj) {
  // deserialize self
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto isSeparated = std::get<2>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<3>(commonTuple);

  if (!jObj.contains("exprs")) {
    return tl::make_unexpected(fmt::format("Exprs not specified in ProjectPOp JSON '{}'", jObj));
  }
  std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>> exprs;
  auto exprsJArr = jObj["exprs"].get<std::vector<json>>();
  for (const auto &exprJObj: exprsJArr) {
    auto expExpr = fpdb::expression::gandiva::Expression::fromJson(exprJObj);
    if (!expExpr.has_value()) {
      return tl::make_unexpected(expExpr.error());
    }
    exprs.emplace_back(*expExpr);
  }

  if (!jObj.contains("exprNames")) {
    return tl::make_unexpected(fmt::format("ExprNames not specified in ProjectPOp JSON '{}'", jObj));
  }
  auto exprNames = jObj["exprNames"].get<std::vector<std::string>>();

  if (!jObj.contains("projectColumnNamePairs")) {
    return tl::make_unexpected(fmt::format("ProjectColumnNamePairs not specified in ProjectPOp JSON '{}'", jObj));
  }
  auto projectColumnPairs = jObj["projectColumnNamePairs"].get<std::vector<std::pair<std::string, std::string>>>();

  std::shared_ptr<PhysicalOp> projectPOp = std::make_shared<project::ProjectPOp>(name,
                                                                                 projectColumnNames,
                                                                                 0,
                                                                                 exprs,
                                                                                 exprNames,
                                                                                 projectColumnPairs);
  projectPOp->setSeparated(isSeparated);
  projectPOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);
  physicalOps_.emplace(projectPOp->name(), projectPOp);

  // deserialize producers
  auto expProducers = deserializeProducers(jObj);
  if (!expProducers.has_value()) {
    return tl::make_unexpected(expProducers.error());
  }

  // connect
  PrePToPTransformerUtil::connectManyToOne(*expProducers, projectPOp);

  return projectPOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeAggregatePOp(const ::nlohmann::json &jObj) {
  // deserialize self
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto isSeparated = std::get<2>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<3>(commonTuple);

  if (!jObj.contains("functions")) {
    return tl::make_unexpected(fmt::format("Aggregate functions not specified in AggregatePOp JSON '{}'", jObj));
  }
  std::vector<std::shared_ptr<aggregate::AggregateFunction>> functions;
  auto functionsJArr = jObj["functions"].get<std::vector<json>>();
  for (const auto &functionJObj: functionsJArr) {
    auto expFunction = aggregate::AggregateFunction::fromJson(functionJObj);
    if (!expFunction.has_value()) {
      return tl::make_unexpected(expFunction.error());
    }
    functions.emplace_back(*expFunction);
  }

  std::shared_ptr<PhysicalOp> aggregatePOp = std::make_shared<aggregate::AggregatePOp>(name,
                                                                                       projectColumnNames,
                                                                                       0,
                                                                                       functions);
  aggregatePOp->setSeparated(isSeparated);
  aggregatePOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);
  physicalOps_.emplace(aggregatePOp->name(), aggregatePOp);

  // deserialize producers
  auto expProducers = deserializeProducers(jObj);
  if (!expProducers.has_value()) {
    return tl::make_unexpected(expProducers.error());
  }

  // connect
  PrePToPTransformerUtil::connectManyToOne(*expProducers, aggregatePOp);

  return aggregatePOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeShufflePOp(const ::nlohmann::json &jObj) {
  // deserialize self
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto isSeparated = std::get<2>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<3>(commonTuple);

  if (!jObj.contains("shuffleColumnNames")) {
    return tl::make_unexpected(fmt::format("ShuffleColumnNames not specified in ShufflePOp JSON '{}'", jObj));
  }
  auto shuffleColumnNames = jObj["shuffleColumnNames"].get<std::vector<std::string>>();

  if (!jObj.contains("consumerVec")) {
    return tl::make_unexpected(fmt::format("ConsumerVec not specified in ShufflePOp JSON '{}'", jObj));
  }
  auto consumerVec = jObj["consumerVec"].get<std::vector<std::string>>();

  std::shared_ptr<PhysicalOp> shufflePOp = std::make_shared<shuffle::ShufflePOp>(name,
                                                                                 projectColumnNames,
                                                                                 0,
                                                                                 shuffleColumnNames,
                                                                                 consumerVec);
  shufflePOp->setSeparated(isSeparated);
  shufflePOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);
  physicalOps_.emplace(shufflePOp->name(), shufflePOp);

  // deserialize producers
  auto expProducers = deserializeProducers(jObj);
  if (!expProducers.has_value()) {
    return tl::make_unexpected(expProducers.error());
  }

  // connect
  PrePToPTransformerUtil::connectManyToOne(*expProducers, shufflePOp);

  return shufflePOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeCollatePOp(const ::nlohmann::json &jObj) {
  // deserialize self
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto isSeparated = std::get<2>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<3>(commonTuple);

  std::shared_ptr<PhysicalOp> collatePOp = std::make_shared<fpdb::executor::physical::collate::CollatePOp>(
          name,
          projectColumnNames,
          0);
  collatePOp->setSeparated(isSeparated);
  collatePOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);
  physicalOps_.emplace(collatePOp->name(), collatePOp);

  // deserialize producers
  auto expProducers = deserializeProducers(jObj);
  if (!expProducers.has_value()) {
    return tl::make_unexpected(expProducers.error());
  }

  // connect, need to handle specially when producer is ShufflePOp,
  // because collatePOp shouldn't be added to its consumerVec
  for (const auto &producer: *expProducers) {
    if (producer->getType() == POpType::SHUFFLE) {
      auto shufflePOp = std::static_pointer_cast<shuffle::ShufflePOp>(producer);
      auto consumerVec = shufflePOp->getConsumerVec();
      shufflePOp->produce(collatePOp);
      collatePOp->consume(shufflePOp);
      shufflePOp->setConsumerVec(consumerVec);
    } else {
      producer->produce(collatePOp);
      collatePOp->consume(producer);
    }
  }

  return collatePOp;
}

tl::expected<std::tuple<std::string,
                        std::vector<std::string>,
                        bool,
                        std::unordered_map<std::string, std::shared_ptr<fpdb_store::FPDBStoreBloomFilterUseInfo>>>,
             std::string>
PhysicalPlanDeserializer::deserializePOpCommon(const ::nlohmann::json &jObj) {
  if (!jObj.contains("name")) {
    return tl::make_unexpected(fmt::format("Name not specified in physical operator JSON '{}'", jObj));
  }
  auto name = jObj["name"].get<std::string>();

  if (!jObj.contains("projectColumnNames")) {
    return tl::make_unexpected(fmt::format("ProjectColumnNames not specified in physical operator JSON '{}'", jObj));
  }
  auto projectColumnNames = jObj["projectColumnNames"].get<std::vector<std::string>>();

  if (!jObj.contains("isSeparated")) {
    return tl::make_unexpected(fmt::format("IsSeparated not specified in physical operator JSON '{}'", jObj));
  }
  auto isSeparated = jObj["isSeparated"].get<bool>();

  if (!jObj.contains("consumerToBloomFilterInfo")) {
    return tl::make_unexpected(
            fmt::format("ConsumerToBloomFilterInfo not specified in physical operator JSON '{}'", jObj));
  }
  std::unordered_map<std::string, std::shared_ptr<fpdb_store::FPDBStoreBloomFilterUseInfo>> consumerToBloomFilterInfo;
  const auto &consumerToBloomFilterInfoJMap =
          jObj["consumerToBloomFilterInfo"].get<std::unordered_map<std::string, json>>();
  for (const auto &consumerToBloomFilterInfoJIt: consumerToBloomFilterInfoJMap) {
    auto expBloomFilterInfo = fpdb_store::FPDBStoreBloomFilterUseInfo::fromJson(consumerToBloomFilterInfoJIt.second);
    if (!expBloomFilterInfo.has_value()) {
      return tl::make_unexpected(expBloomFilterInfo.error());
    }
    consumerToBloomFilterInfo.emplace(consumerToBloomFilterInfoJIt.first, *expBloomFilterInfo);
  }

  return std::tuple<std::string, std::vector<std::string>, bool,
                    std::unordered_map<std::string, std::shared_ptr<fpdb_store::FPDBStoreBloomFilterUseInfo>>>
                    {name, projectColumnNames, isSeparated, consumerToBloomFilterInfo};
}

}
