//
// Created by Yifei Yang on 12/10/21.
//

#include <normal/expression/gandiva/InInt32.h>
#include <fmt/format.h>
#include <sstream>

namespace normal::expression::gandiva {

InInt32::InInt32(const shared_ptr<Expression> &left, const unordered_set<int32_t> &values):
  In(left),
  values_(values) {}

void InInt32::compile(const shared_ptr<arrow::Schema> &schema) {
  left_->compile(schema);
  returnType_ = arrow::boolean();

  const auto &leftGandivaExpr = left_->getGandivaExpression();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionInt32(leftGandivaExpr, values_);
}

string InInt32::alias() {
  stringstream ss;
  const auto &leftAlias = fmt::format("cast({} as int)",left_->alias());
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

string InInt32::getTypeString() {
  return "InInt32";
}

shared_ptr<Expression> inInt32_(const shared_ptr<Expression> &left, const unordered_set<int32_t> &values) {
  return make_shared<InInt32>(left, values);
}

}
