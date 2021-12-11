//
// Created by Yifei Yang on 12/10/21.
//

#include <normal/expression/gandiva/InDouble.h>
#include <fmt/format.h>
#include <sstream>

namespace normal::expression::gandiva {

InDouble::InDouble(const shared_ptr<Expression> &left, const unordered_set<double> &values):
  In(left),
  values_(values) {}

void InDouble::compile(const shared_ptr<arrow::Schema> &schema) {
  left_->compile(schema);
  returnType_ = arrow::boolean();

  const auto &leftGandivaExpr = left_->getGandivaExpression();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionDouble(leftGandivaExpr, values_);
}

string InDouble::alias() {
  stringstream ss;
  const auto &leftAlias = fmt::format("cast({} as float)",left_->alias());
  uint i = 0;
  for (const auto &value: values_) {
    ss << fmt::format("({} = {})", leftAlias, value);
    if (i < values_.size() - 1) {
      ss << " or ";
    }
    ++i;
  }
  return ss.str();
}

string InDouble::getTypeString() {
  return "InDouble";
}

shared_ptr<Expression> inDouble_(const shared_ptr<Expression> &left, const unordered_set<double> &values) {
  return make_shared<InDouble>(left, values);
}

}
