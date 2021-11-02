//
// Created by Yifei Yang on 11/1/21.
//

#include <normal/plan/calcite/CalcitePlanJsonDeserializer.h>
#include <normal/tuple/ColumnName.h>

using namespace normal::plan::prephysical;
using namespace normal::tuple;

namespace normal::plan::calcite {

void CalcitePlanJsonDeserializer::deserialize(const string &planJsonString) {
  auto jObj = json::parse(planJsonString);
  auto rootPrePOp = deserializeDfs(jObj);
  int a = 1;
}

shared_ptr<PrePhysicalOp> CalcitePlanJsonDeserializer::deserializeDfs(const json &jObj) {
  string opName = jObj["operator"].get<string>();
  if (opName == "EnumerableSort") {
    return deserializeSort(jObj);
  } else if (opName == "EnumerableAggregate") {
    return deserializeAggregateOrGroup(jObj);
  } else {
    return nullptr;
//    throw runtime_error("Unsupported PrePhysicalOp: " + opName);
  }
}

vector<shared_ptr<PrePhysicalOp>> CalcitePlanJsonDeserializer::deserializeProducers(const json &jObj) {
  vector<shared_ptr<PrePhysicalOp>> producers;
  auto producersJArr = jObj["inputs"].get<vector<json>>();
  for (const auto &producerJObj: producersJArr) {
    shared_ptr<PrePhysicalOp> producer = deserializeDfs(producerJObj);
    producers.emplace_back(producer);
  }
  return producers;
}

shared_ptr<SortPrePOp> CalcitePlanJsonDeserializer::deserializeSort(const json &jObj) {
  // deserialize sort fields
  auto sortFieldsJArr = jObj["sortFields"].get<vector<json>>();
  vector<pair<string, FieldDirection>> sortFields;
  for (const auto &sortFieldJObj: sortFieldsJArr) {
    string columnName = ColumnName::canonicalize(sortFieldJObj["field"].get<string>());
    string directionStr = sortFieldJObj["direction"].get<string>();
    auto direction = (directionStr == "ASCENDING") ? ASC : DESC;
    sortFields.emplace_back(make_pair(columnName, direction));
  }
  shared_ptr<SortPrePOp> sortPrePOp = make_shared<SortPrePOp>(sortFields);

  // deserialize producers
  sortPrePOp->setProducers(deserializeProducers(jObj));
  return sortPrePOp;
}

shared_ptr<PrePhysicalOp> CalcitePlanJsonDeserializer::deserializeAggregateOrGroup(const json &jObj) {
  // deserialize group fields
  auto groupFieldsJArr = jObj["groupFields"].get<vector<json>>();
  vector<string> groupColumnNames;
  groupColumnNames.reserve(groupFieldsJArr.size());
  for (const auto &groupFieldJObj: groupFieldsJArr) {
    string groupColumnName = ColumnName::canonicalize(groupFieldJObj.get<string>());
    groupColumnNames.emplace_back(groupColumnName);
  }

  // deserialize aggregate columns and functions
  auto aggregationsJArr = jObj["aggregations"].get<vector<json>>();
  vector<string> aggregateColumnNames;
  aggregateColumnNames.reserve(aggregationsJArr.size());
  for (const auto &aggregationJObj: aggregationsJArr) {
    string aggregateColumnName = ColumnName::canonicalize(aggregationJObj["aggField"].get<string>());
    aggregateColumnNames.emplace_back(aggregateColumnName);
  }


  return shared_ptr<AggregatePrePOp>();
}
  
shared_ptr<AggregatePrePOp> CalcitePlanJsonDeserializer::deserializeAggregate(const json &jObj) {
  return shared_ptr<AggregatePrePOp>();
}

shared_ptr<GroupPrePOp> CalcitePlanJsonDeserializer::deserializeGroup(const json &jObj) {
  return shared_ptr<GroupPrePOp>();
}

}