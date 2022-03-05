//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/serialization/PhysicalPlanDeserializer.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreFileScanPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>

using json = ::nlohmann::json;

namespace fpdb::executor::physical {

PhysicalPlanDeserializer::PhysicalPlanDeserializer(const std::string &planString,
                                                   const std::string &storeRootPath):
  planString_(planString),
  storeRootPath_(storeRootPath) {}

tl::expected<std::shared_ptr<PhysicalPlan>, std::string> PhysicalPlanDeserializer::deserialize() {
  try {
    // parse
    auto jObj = json::parse(planString_);
    if (!jObj.is_object()) {
      return tl::make_unexpected(fmt::format("Cannot parse physical plan JSON '{}", planString_));
    }

    // deserialize in DFS order
    auto deserializeRes = deserializeDfs(jObj);
    if (!deserializeRes.has_value()) {
      return tl::make_unexpected(deserializeRes.error());
    }

    return std::make_shared<PhysicalPlan>(physicalOps_);
  } catch(nlohmann::json::parse_error& ex) {
    return tl::make_unexpected(ex.what());
  }
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string> PhysicalPlanDeserializer::deserializeDfs(::nlohmann::json jObj) {
  if (!jObj.contains("type")) {
    return tl::make_unexpected(fmt::format("Type not specified in physical operator JSON '{}'", jObj));
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
  } else if (type == "CollatePOp") {
    return deserializeCollatePOp(jObj);
  } else {
    return tl::make_unexpected(fmt::format("Unsupported physical operator type at store: '{}'", type));
  }
}

tl::expected<std::vector<std::shared_ptr<PhysicalOp>>, std::string>
PhysicalPlanDeserializer::deserializeProducers(::nlohmann::json jObj) {
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
PhysicalPlanDeserializer::deserializeFPDBStoreFileScanPOp(::nlohmann::json jObj) {
  // deserialize self
  if (!jObj.contains("name")) {
    return tl::make_unexpected(fmt::format("Name not specified in FileScanPOp JSON '{}'", jObj));
  }
  auto name = jObj["name"].get<std::string>();
  
  if (!jObj.contains("projectColumnNames")) {
    return tl::make_unexpected(fmt::format("ProjectColumnNames not specified in FileScanPOp JSON '{}'", jObj));
  }
  auto projectColumnNames = jObj["projectColumnNames"].get<std::vector<std::string>>();

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
                                                                                                    byteRange);
  physicalOps_.emplace_back(storeFileScanPOp);
  
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
PhysicalPlanDeserializer::deserializeFilterPOp(::nlohmann::json jObj) {
  // deserialize self
  if (!jObj.contains("name")) {
    return tl::make_unexpected(fmt::format("Name not specified in FilterPOp JSON '{}'", jObj));
  }
  auto name = jObj["name"].get<std::string>();

  if (!jObj.contains("projectColumnNames")) {
    return tl::make_unexpected(fmt::format("ProjectColumnNames not specified in FilterPOp JSON '{}'", jObj));
  }
  auto projectColumnNames = jObj["projectColumnNames"].get<std::vector<std::string>>();

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
  physicalOps_.emplace_back(filterPOp);

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
PhysicalPlanDeserializer::deserializeProjectPOp(::nlohmann::json jObj) {
  // deserialize self
  if (!jObj.contains("name")) {
    return tl::make_unexpected(fmt::format("Name not specified in ProjectPOp JSON '{}'", jObj));
  }
  auto name = jObj["name"].get<std::string>();

  if (!jObj.contains("projectColumnNames")) {
    return tl::make_unexpected(fmt::format("ProjectColumnNames not specified in ProjectPOp JSON '{}'", jObj));
  }
  auto projectColumnNames = jObj["projectColumnNames"].get<std::vector<std::string>>();

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
  physicalOps_.emplace_back(projectPOp);

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
PhysicalPlanDeserializer::deserializeAggregatePOp(::nlohmann::json jObj) {
  // deserialize self
  if (!jObj.contains("name")) {
    return tl::make_unexpected(fmt::format("Name not specified in AggregatePOp JSON '{}'", jObj));
  }
  auto name = jObj["name"].get<std::string>();

  if (!jObj.contains("projectColumnNames")) {
    return tl::make_unexpected(fmt::format("ProjectColumnNames not specified in AggregatePOp JSON '{}'", jObj));
  }
  auto projectColumnNames = jObj["projectColumnNames"].get<std::vector<std::string>>();

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
  physicalOps_.emplace_back(aggregatePOp);

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
PhysicalPlanDeserializer::deserializeCollatePOp(::nlohmann::json jObj) {
  // deserialize self
  if (!jObj.contains("name")) {
    return tl::make_unexpected(fmt::format("Name not specified in CollatePOp JSON '{}'", jObj));
  }
  auto name = jObj["name"].get<std::string>();

  if (!jObj.contains("projectColumnNames")) {
    return tl::make_unexpected(fmt::format("ProjectColumnNames not specified in CollatePOp JSON '{}'", jObj));
  }
  auto projectColumnNames = jObj["projectColumnNames"].get<std::vector<std::string>>();

  std::shared_ptr<PhysicalOp> collatePOp = std::make_shared<fpdb::executor::physical::collate::CollatePOp>(
          name,
          projectColumnNames,
          0);
  physicalOps_.emplace_back(collatePOp);

  // deserialize producers
  auto expProducers = deserializeProducers(jObj);
  if (!expProducers.has_value()) {
    return tl::make_unexpected(expProducers.error());
  }

  // connect
  PrePToPTransformerUtil::connectManyToOne(*expProducers, collatePOp);

  return collatePOp;
}

}
