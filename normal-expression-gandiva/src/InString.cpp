//
// Created by Yifei Yang on 12/10/21.
//

#include <normal/expression/gandiva/InString.h>
#include <fmt/format.h>
#include <sstream>

namespace normal::expression::gandiva {

InString::InString(const shared_ptr<Expression> &left, const unordered_set<string> &values):
  In(left),
  values_(values) {}

void InString::compile(const shared_ptr<arrow::Schema> &schema) {
  left_->compile(schema);
  returnType_ = arrow::boolean();

  const auto &leftGandivaExpr = left_->getGandivaExpression();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionString(leftGandivaExpr, values_);
}

string InString::alias() {
  stringstream ss;
  const auto &leftAlias = left_->alias();
  uint i = 0;
  for (const auto &value: values_) {
    ss << fmt::format("({} = \'{}\')", leftAlias, value);
    if (i < values_.size() - 1) {
      ss << " or ";
    }
    ++i;
  }
  return ss.str();
}

string InString::getTypeString() {
  return "InString";
}

shared_ptr<Expression> inString_(const shared_ptr<Expression> &left, const unordered_set<string> &values) {
  return make_shared<InString>(left, values);
}

}
