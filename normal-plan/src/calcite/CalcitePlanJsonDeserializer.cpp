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

#include <unordered_set>

using namespace normal::plan::prephysical;
using namespace normal::tuple;

namespace normal::plan::calcite {

void CalcitePlanJsonDeserializer::deserialize(const string &planJsonString) {
  auto jObj = json::parse(planJsonString);
  auto rootPrePOp = deserializeDfs(jObj);
  int a = 1;
}

shared_ptr<PrePhysicalOp> CalcitePlanJsonDeserializer::deserializeDfs(json &jObj) {
  string opName = jObj["operator"].get<string>();
  if (opName == "EnumerableSort") {
    return deserializeSort(jObj);
  } else if (opName == "EnumerableAggregate") {
    return deserializeAggregateOrGroup(jObj);
  } else if (opName == "EnumerableProject") {
    return deserializeProject(jObj);
  } else {
//    return nullptr;
    throw runtime_error("Unsupported PrePhysicalOp: " + opName);
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
      throw runtime_error("Unsupported expression type: " + opName);
    }
  }

  else {
    throw runtime_error("Unsupported expression: neither a column or an operation");
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
      throw runtime_error("Unsupported aggregation function type: " + aggFunctionStr);
    }

    // aggregate expr
    shared_ptr<Expression> aggFieldExpr;
    const string &aggInputColumnName = ColumnName::canonicalize(aggregationJObj["aggInputField"].get<string>());
    if (aggInputColumnName.substr(0, 2) == "$f") {
      // it's not just a column, need to get it from the input Project op
      auto inputProjectJObj = jObj["inputs"].get<vector<json>>()[0];
      int aggInputProjectFieldId = stoi(aggInputColumnName.substr(2, aggInputColumnName.length() - 2));
      auto aggFieldExprJObj = inputProjectJObj["fields"].get<vector<json>>()[aggInputProjectFieldId];
      aggFieldExpr = deserializeExpression(aggFieldExprJObj);

      // need to let the input Project op know the expr has been consumed
      json consumedProjectFieldIdJArr;
      if (inputProjectJObj.contains("consumedFieldsId")) {
        consumedProjectFieldIdJArr = inputProjectJObj["consumedFieldsId"].get<vector<int>>();
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

shared_ptr<prephysical::PrePhysicalOp> CalcitePlanJsonDeserializer::deserializeProject(const json &jObj) {
  // whether we can skip to deserialize the Project
  bool skipSelf = true;

  // only when there is an expr, we need to keep the Project
  if (jObj.contains("consumedFieldsId")) {
    const vector<int> &consumedFieldsIdVec = jObj["consumedFieldsId"].get<vector<int>>();
    unordered_set<int> consumedFieldsIdSet(consumedFieldsIdVec.begin(), consumedFieldsIdVec.end());
    // check each project field
    const vector<json> &fieldsJArr = jObj["fields"].get<vector<json>>();
    for (int i = 0; i < fieldsJArr.size(); ++i) {
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

  // project columns
  unordered_set<string> projectColumnNames;
  const auto &fieldsJArr = jObj["fields"].get<vector<json>>();
  for (const auto &fieldJObj: fieldsJArr) {
    if (fieldJObj.contains("inputRef")) {
      const string &projectColumnName = ColumnName::canonicalize(fieldJObj["inputRef"].get<string>());
      projectColumnNames.emplace(projectColumnName);
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
    vector<shared_ptr<Expression>> exprs;
    for (const auto &fieldJObj: fieldsJArr) {
      if (fieldJObj.contains("op")) {
        exprs.emplace_back(deserializeExpression(fieldJObj));
      }
    }
    shared_ptr<ProjectPrePOp> projectPrePOp = make_shared<ProjectPrePOp>(exprs);

    // deserialize producers
    projectPrePOp->setProducers(deserializeProducers(jObj));
    return projectPrePOp;
  }
}

shared_ptr<prephysical::HashJoinPrePOp> CalcitePlanJsonDeserializer::deserializeHashJoin(const json &jObj) {
  return shared_ptr<prephysical::HashJoinPrePOp>();
}

}