//
// Created by Yifei Yang on 11/1/21.
//

#include <normal/plan/calcite/CalcitePlanJsonDeserializer.h>
#include <normal/plan/prephysical/JoinType.h>
#include <normal/catalogue/s3/S3CatalogueEntry.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/Or.h>
#include <normal/expression/gandiva/Add.h>
#include <normal/expression/gandiva/Subtract.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/expression/gandiva/Divide.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/StringLiteral.h>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <normal/tuple/ColumnName.h>

#include <fmt/format.h>
#include <unordered_set>
#include <utility>

using namespace normal::catalogue;
using namespace normal::tuple;

namespace normal::plan::calcite {

CalcitePlanJsonDeserializer::CalcitePlanJsonDeserializer(string planJsonString,
                                                         const shared_ptr<CatalogueEntry> &catalogueEntry)
        : planJsonString_(std::move(planJsonString)), catalogueEntry_(catalogueEntry) {}

shared_ptr<PrePhysicalPlan> CalcitePlanJsonDeserializer::deserialize() {
  auto jObj = json::parse(planJsonString_);
  auto rootPrePOp = deserializeDfs(jObj);
  return make_shared<PrePhysicalPlan>(rootPrePOp);
}

shared_ptr<PrePhysicalOp> CalcitePlanJsonDeserializer::deserializeDfs(json &jObj) {
  string opName = jObj["operator"].get<string>();
  if (opName == "EnumerableSort") {
    return deserializeSort(jObj);
  } else if (opName == "EnumerableAggregate") {
    return deserializeAggregateOrGroup(jObj);
  } else if (opName == "EnumerableProject") {
    return deserializeProject(jObj);
  } else if (opName == "EnumerableHashJoin") {
    return deserializeHashJoin(jObj);
  } else if (opName == "EnumerableFilter") {
    return deserializeFilterOrFilterableScan(jObj);
  } else if (opName == "EnumerableTableScan") {
    return deserializeTableScan(jObj);
  } else {
    throw runtime_error(fmt::format("Unsupported PrePhysicalOp type, {}", opName));
  }
}

vector<shared_ptr<PrePhysicalOp>> CalcitePlanJsonDeserializer::deserializeProducers(const json &jObj) {
  vector<shared_ptr<PrePhysicalOp>> producers;
  auto producersJArr = jObj["inputs"].get<vector<json>>();
  for (auto &producerJObj: producersJArr) {
    const shared_ptr<PrePhysicalOp> &producer = deserializeDfs(producerJObj);
    producers.emplace_back(producer);
  }
  return producers;
}

shared_ptr<Expression> CalcitePlanJsonDeserializer::deserializeExpression(const json &jObj) {
  // single column
  if (jObj.contains("inputRef")) {
    const auto &columnName = ColumnName::canonicalize(jObj["inputRef"].get<string>());
    return col(columnName);
  }

  // literal
  else if (jObj.contains("literal")) {
    const auto &literalJObj = jObj["literal"];
    const auto &type = literalJObj["type"].get<string>();
    if (type == "VARCHAR") {
      const string &value = literalJObj["value"].get<string>();
      return str_lit(value);
    } else if (type == "INTEGER") {
      const int value = literalJObj["value"].get<int>();
      return num_lit<arrow::Int32Type>(value);
    } else if (type == "BIGINT") {
      const long value = literalJObj["value"].get<long>();
      return num_lit<arrow::Int64Type>(value);
    } else if (type == "DECIMAL") {
      const double value = literalJObj["value"].get<double>();
      return num_lit<arrow::DoubleType>(value);
    } else {
      throw runtime_error(fmt::format("Unsupported literal type, {}, from: {}", type, to_string(literalJObj)));
    }
  }

  // binary operation
  else if (jObj.contains("op")) {
    const string &opName = jObj["op"].get<string>();
    if (opName == "AND" || opName == "OR" || opName == "PLUS" || opName == "MINUS" || opName == "TIMES"
        || opName == "DIVIDE" || opName == "EQUALS" || opName == "LESS_THAN" || opName == "GREATER_THAN"
        || opName == "LESS_THAN_OR_EQUAL" || opName == "GREATER_THAN_OR_EQUAL") {
      const auto &leftExpr = deserializeExpression(jObj["operands"].get<vector<json>>()[0]);
      const auto &rightExpr = deserializeExpression(jObj["operands"].get<vector<json>>()[1]);
      if (opName == "AND") {
        return and_(leftExpr, rightExpr);
      } else if (opName == "OR") {
        return or_(leftExpr, rightExpr);
      } else if (opName == "PLUS") {
        return normal::expression::gandiva::plus(leftExpr, rightExpr);
      } else if (opName == "MINUS") {
        return normal::expression::gandiva::minus(leftExpr, rightExpr);
      } else if (opName == "TIMES") {
        return times(leftExpr, rightExpr);
      } else if (opName == "DIVIDE") {
        return divide(leftExpr, rightExpr);
      } else if (opName == "EQUALS") {
        return eq(leftExpr, rightExpr);
      } else if (opName == "LESS_THAN") {
        return lt(leftExpr, rightExpr);
      } else if (opName == "GREATER_THAN") {
        return gt(leftExpr, rightExpr);
      } else if (opName == "LESS_THAN_OR_EQUAL") {
        return lte(leftExpr, rightExpr);
      } else {
        return gte(leftExpr, rightExpr);
      }
    } else {
      throw runtime_error(fmt::format("Unsupported expression type, {}, from: {}", opName, to_string(jObj)));
    }
  }

  else {
    throw runtime_error(fmt::format("Unsupported expression type, only column, literal and operation are "
                                    "supported, from: {}", to_string(jObj)));
  }
}

pair<vector<string>, vector<string>> CalcitePlanJsonDeserializer::deserializeHashJoinCondition(const json &jObj) {
  const string &opName = jObj["op"].get<string>();
  if (opName == "AND") {
    vector<string> leftColumnNames, rightColumnNames;
    auto childJObj1 = jObj["operands"].get<vector<json>>()[0];
    auto childJObj2 = jObj["operands"].get<vector<json>>()[1];
    const auto &childJoinColumns1 = deserializeHashJoinCondition(childJObj1);
    const auto &childJoinColumns2 = deserializeHashJoinCondition(childJObj2);
    leftColumnNames.insert(leftColumnNames.end(), childJoinColumns1.first.begin(), childJoinColumns1.first.end());
    leftColumnNames.insert(leftColumnNames.end(), childJoinColumns2.first.begin(), childJoinColumns2.first.end());
    rightColumnNames.insert(rightColumnNames.end(), childJoinColumns1.second.begin(), childJoinColumns1.second.end());
    rightColumnNames.insert(rightColumnNames.end(), childJoinColumns2.second.begin(), childJoinColumns2.second.end());
    return make_pair(leftColumnNames, rightColumnNames);
  }

  else if (opName == "EQUALS") {
    auto leftJObj = jObj["operands"].get<vector<json>>()[0];
    auto rightJObj = jObj["operands"].get<vector<json>>()[1];
    auto leftColumnName = leftJObj["inputRef"].get<string>();
    auto rightColumnName = rightJObj["inputRef"].get<string>();
    return make_pair(vector<string>{leftColumnName}, vector<string>{rightColumnName});
  }

  else {
    throw runtime_error(fmt::format("Invalid hash join condition operation type, {}, from: {}",
                                    opName, to_string(jObj)));
  }
}

shared_ptr<SortPrePOp> CalcitePlanJsonDeserializer::deserializeSort(const json &jObj) {
  // deserialize sort fields
  const auto &sortFieldsJArr = jObj["sortFields"].get<vector<json>>();
  vector<pair<string, FieldDirection>> sortFields;
  for (const auto &sortFieldJObj: sortFieldsJArr) {
    const string &columnName = ColumnName::canonicalize(sortFieldJObj["field"].get<string>());
    const string &directionStr = sortFieldJObj["direction"].get<string>();
    auto direction = (directionStr == "ASCENDING") ? ASC : DESC;
    sortFields.emplace_back(make_pair(columnName, direction));
  }
  shared_ptr<SortPrePOp> sortPrePOp = make_shared<SortPrePOp>(sortFields);

  // deserialize producers
  sortPrePOp->setProducers(deserializeProducers(jObj));
  return sortPrePOp;
}

shared_ptr<PrePhysicalOp> CalcitePlanJsonDeserializer::deserializeAggregateOrGroup(json &jObj) {
  // deserialize group fields
  const auto &groupFieldsJArr = jObj["groupFields"].get<vector<json>>();
  vector<string> groupColumnNames;
  groupColumnNames.reserve(groupFieldsJArr.size());
  for (const auto &groupFieldJObj: groupFieldsJArr) {
    const string &groupColumnName = ColumnName::canonicalize(groupFieldJObj.get<string>());
    groupColumnNames.emplace_back(groupColumnName);
  }

  // deserialize aggregate output columns and functions
  const auto &aggregationsJArr = jObj["aggregations"].get<vector<json>>();
  vector<string> aggOutputColumnNames;
  vector<shared_ptr<AggregatePrePFunction>> aggFunctions;
  aggOutputColumnNames.reserve(aggregationsJArr.size());
  aggFunctions.reserve(aggregationsJArr.size());

  for (const auto &aggregationJObj: aggregationsJArr) {
    // aggregate output column
    const string &aggOutputColumnName = ColumnName::canonicalize(aggregationJObj["aggOutputField"].get<string>());
    aggOutputColumnNames.emplace_back(aggOutputColumnName);

    // aggregate function type
    const string &aggFunctionStr = aggregationJObj["aggFunction"].get<string>();
    AggregatePrePFunctionType aggFunctionType;
    if (aggFunctionStr == "SUM0" || aggFunctionStr == "SUM") {
      aggFunctionType = SUM;
    } else {
      throw runtime_error(fmt::format("Unsupported aggregation function type, {}, from: {}",
                                      aggFunctionStr, to_string(aggregationJObj)));
    }

    // aggregate expr
    shared_ptr<Expression> aggFieldExpr;
    const string &aggInputColumnName = ColumnName::canonicalize(aggregationJObj["aggInputField"].get<string>());
    if (aggInputColumnName.substr(0, 2) == "$f") {
      // it's not just a column, need to get it from the input Project op
      auto inputProjectJObj = jObj["inputs"].get<vector<json>>()[0];
      size_t aggInputProjectFieldId = stoul(aggInputColumnName.substr(2, aggInputColumnName.length() - 2));
      auto aggFieldExprJObj = inputProjectJObj["fields"].get<vector<json>>()[aggInputProjectFieldId];
      aggFieldExpr = deserializeExpression(aggFieldExprJObj);

      // need to let the input Project op know the expr has been consumed
      json consumedProjectFieldIdJArr;
      if (inputProjectJObj.contains("consumedFieldsId")) {
        consumedProjectFieldIdJArr = inputProjectJObj["consumedFieldsId"].get<vector<size_t>>();
      }
      consumedProjectFieldIdJArr.emplace_back(aggInputProjectFieldId);
      inputProjectJObj["consumedFieldsId"] = consumedProjectFieldIdJArr;
      jObj["inputs"] = vector<json>{inputProjectJObj};
    } else {
      // it's just a column
      aggFieldExpr = col(aggInputColumnName);
    }

    // make the aggregate function
    shared_ptr<AggregatePrePFunction> aggFunction = make_shared<AggregatePrePFunction>(aggFunctionType, aggFieldExpr);
    aggFunctions.emplace_back(aggFunction);
  }

  // project column names
  unordered_set<string> projectColumnNames;
  projectColumnNames.insert(aggOutputColumnNames.begin(), aggOutputColumnNames.end());

  // decide if it's an Aggregate op or a Group op
  shared_ptr<PrePhysicalOp> prePOp;
  if (groupColumnNames.empty()) {
    prePOp = make_shared<AggregatePrePOp>(aggOutputColumnNames, aggFunctions);
  } else {
    projectColumnNames.insert(groupColumnNames.begin(), groupColumnNames.end());
    prePOp = make_shared<GroupPrePOp>(groupColumnNames, aggOutputColumnNames, aggFunctions);
  }
  prePOp->setProjectColumnNames(projectColumnNames);

  // deserialize producers
  prePOp->setProducers(deserializeProducers(jObj));
  return prePOp;
}

shared_ptr<prephysical::PrePhysicalOp> CalcitePlanJsonDeserializer::deserializeProject(const json &jObj) {
  // whether we can skip to deserialize the Project
  bool skipSelf = true;

  // only when there is an expr, we need to keep the Project
  if (jObj.contains("consumedFieldsId")) {
    const vector<size_t> &consumedFieldsIdVec = jObj["consumedFieldsId"].get<vector<size_t>>();
    unordered_set<size_t> consumedFieldsIdSet(consumedFieldsIdVec.begin(), consumedFieldsIdVec.end());
    // check each project field
    const vector<json> &fieldsJArr = jObj["fields"].get<vector<json>>();
    for (size_t i = 0; i < fieldsJArr.size(); ++i) {
      if (consumedFieldsIdSet.find(i) != consumedFieldsIdSet.end()) {
        // this field is consumed by the consumer
        continue;
      }
      const auto &fieldJObj = fieldsJArr[i];
      if (fieldJObj.contains("inputRef")) {
        // the field is just a column, not an expr
        continue;
      }
      // otherwise the Project is needed
      skipSelf = false;
      break;
    }
  }

  // project columns and exprs
  unordered_set<string> projectColumnNames;
  vector<shared_ptr<Expression>> exprs;
  const auto &fieldsJArr = jObj["fields"].get<vector<json>>();
  for (const auto &fieldJObj: fieldsJArr) {
    if (fieldJObj.contains("inputRef")) {
      const string &projectColumnName = ColumnName::canonicalize(fieldJObj["inputRef"].get<string>());
      projectColumnNames.emplace(projectColumnName);
    } else if (fieldJObj.contains("op")) {
      const auto &expr = deserializeExpression(fieldJObj);
      exprs.emplace_back(expr);
      const auto &exprUsedColumnNames = expr->involvedColumnNames();
      projectColumnNames.insert(exprUsedColumnNames->begin(), exprUsedColumnNames->end());
    }
  }

  if (skipSelf) {
    // skip the Project, continue to deserialize its producer
    const auto &producer = deserializeDfs(jObj["inputs"].get<vector<json>>()[0]);
    // set projectColumnNames for its producer
    producer->setProjectColumnNames(projectColumnNames);
    return producer;
  }

  else {
    // not skip the Project
    shared_ptr<ProjectPrePOp> projectPrePOp = make_shared<ProjectPrePOp>(exprs);

    // deserialize producers
    projectPrePOp->setProducers(deserializeProducers(jObj));
    return projectPrePOp;
  }
}

shared_ptr<prephysical::HashJoinPrePOp> CalcitePlanJsonDeserializer::deserializeHashJoin(const json &jObj) {
  // deserialize join type
  const auto &joinTypeStr = jObj["joinType"].get<string>();
  JoinType joinType;
  if (joinTypeStr == "INNER") {
    joinType = INNER;
  } else {
    throw runtime_error(fmt::format("Unsupported join type, {}, from: {}", joinTypeStr, to_string(jObj)));
  }

  // deserialize hash join condition
  const auto &joinCondJObj = jObj["condition"];
  const pair<vector<string>, vector<string>> &joinColumnNames = deserializeHashJoinCondition(joinCondJObj);

  shared_ptr<HashJoinPrePOp> hashJoinPrePOp = make_shared<HashJoinPrePOp>(joinType,
                                                                         joinColumnNames.first,
                                                                         joinColumnNames.second);

  // deserialize producers
  hashJoinPrePOp->setProducers(deserializeProducers(jObj));
  return hashJoinPrePOp;
}

shared_ptr<prephysical::PrePhysicalOp> CalcitePlanJsonDeserializer::deserializeFilterOrFilterableScan(const json &jObj) {
  // deserialize filter predicate
  const auto &predicate = deserializeExpression(jObj["condition"]);

  // deserialize producers
  const auto &producers = deserializeProducers(jObj);
  if (producers[0]->getType() == FILTERABLE_SCAN) {
    // if the producer is filterable scan, then set its filter predicate
    const auto &filterableScan = static_pointer_cast<FilterableScanPrePOp>(producers[0]);
    filterableScan->setPredicate(predicate);
    return filterableScan;
  }

  else {
    // else do regularly, make a filter op
    shared_ptr<FilterPrePOp> filterPrePOp = make_shared<FilterPrePOp>(predicate);
    filterPrePOp->setProducers(deserializeProducers(jObj));
    return filterPrePOp;
  }
}

shared_ptr<prephysical::FilterableScanPrePOp> CalcitePlanJsonDeserializer::deserializeTableScan(const json &jObj) {
  const string &tableName = jObj["table"].get<string>();
  shared_ptr<Table> table;

  unordered_set<string> columnNameSet;
  // fetch table from catalogue entry
  if (catalogueEntry_->getType() == S3) {
    const auto s3CatalogueEntry = static_pointer_cast<s3::S3CatalogueEntry>(catalogueEntry_);
    table = s3CatalogueEntry->getS3Table(tableName);
    const vector<string> &columnNames = table->getColumnNames();
    columnNameSet.insert(columnNames.begin(), columnNames.end());
  } else {
    throw runtime_error(fmt::format("Unsupported catalogue entry type: {}, from: {}",
                                    catalogueEntry_->getType(), catalogueEntry_->getName()));
  }

  auto filterableScanPrePOp = make_shared<prephysical::FilterableScanPrePOp>(table);
  filterableScanPrePOp->setProjectColumnNames(columnNameSet);
  return filterableScanPrePOp;
}

}