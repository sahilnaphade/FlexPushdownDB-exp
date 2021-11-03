//
// Created by Yifei Yang on 11/1/21.
//

#include <normal/plan/calcite/CalcitePlanJsonDeserializer.h>
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
//    return nullptr;
    throw runtime_error("Unsupported PrePhysicalOp: " + opName);
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

shared_ptr<Expression> CalcitePlanJsonDeserializer::deserializeExpression(const json &jObj) {
  // single column
  if (jObj.contains("inputRef")) {
    string columnName = ColumnName::canonicalize(jObj["inputRef"].get<string>());
    return col(columnName);
  }

  // binary operation
  else if (jObj.contains("op")) {
    string opName = jObj["op"].get<string>();
    if (opName == "AND" || opName == "OR" || opName == "PLUS" || opName == "MINUS" || opName == "TIMES"
        || opName == "DIVIDE" || opName == "EQUALS" || opName == "LESS_THAN" || opName == "GREATER_THAN"
        || opName == "LESS_THAN_OR_EQUAL" || opName == "GREATER_THAN_OR_EQUAL") {
      auto leftExpr = deserializeExpression(jObj["operands"].get<vector<json>>()[0]);
      auto rightExpr = deserializeExpression(jObj["operands"].get<vector<json>>()[1]);
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
      throw runtime_error("Unsupported expression type: " + opName);
    }
  }

  else {
    throw runtime_error("Unsupported expression: neither a column or an operation");
  }
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

  // deserialize aggregate output columns and functions
  auto aggregationsJArr = jObj["aggregations"].get<vector<json>>();
  vector<string> aggOutputColumnNames;
  vector<shared_ptr<AggregatePrePFunction>> aggFunctions;
  aggOutputColumnNames.reserve(aggregationsJArr.size());
  aggFunctions.reserve(aggregationsJArr.size());

  for (const auto &aggregationJObj: aggregationsJArr) {
    // aggregate output column
    string aggOutputColumnName = ColumnName::canonicalize(aggregationJObj["aggOutputField"].get<string>());
    aggOutputColumnNames.emplace_back(aggOutputColumnName);

    // aggregate function type
    string aggFunctionStr = aggregationJObj["aggFunction"].get<string>();
    AggregatePrePFunctionType aggFunctionType;
    if (aggFunctionStr == "SUM0" || aggFunctionStr == "SUM") {
      aggFunctionType = SUM;
    } else {
      throw runtime_error("Unsupported aggregation function type: " + aggFunctionStr);
    }

    // aggregate expr, need to get it from the input Project op if it's not just a column
    shared_ptr<Expression> aggFieldExpr;
    string aggInputColumnName = ColumnName::canonicalize(aggregationJObj["aggInputField"].get<string>());
    if (aggInputColumnName.substr(0, 2) == "$f") {
      // it's not just a column
      auto inputProjectJObj = jObj["inputs"].get<vector<json>>()[0];
      int aggInputProjectFieldId = stoi(aggInputColumnName.substr(2, aggInputColumnName.length() - 2));
      auto aggFieldExprJObj = inputProjectJObj["fields"].get<vector<json>>()[aggInputProjectFieldId];
      aggFieldExpr = deserializeExpression(aggFieldExprJObj);
    } else {
      // it's just a column
      aggFieldExpr = col(aggInputColumnName);
    }

    // make the aggregate function
    shared_ptr<AggregatePrePFunction> aggFunction = make_shared<AggregatePrePFunction>(aggFunctionType, aggFieldExpr);
    aggFunctions.emplace_back(aggFunction);
  }

  // decide if it's an Aggregate op or a Group op
  shared_ptr<PrePhysicalOp> prePOp;
  if (groupColumnNames.empty()) {
    prePOp = make_shared<AggregatePrePOp>(aggOutputColumnNames, aggFunctions);
  } else {
    prePOp = make_shared<GroupPrePOp>(groupColumnNames, aggOutputColumnNames, aggFunctions);
  }

  // deserialize producers
  prePOp->setProducers(deserializeProducers(jObj));
  return prePOp;
}

}