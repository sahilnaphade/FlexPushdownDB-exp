//
// Created by Yifei Yang on 12/10/21.
//

#include <normal/expression/gandiva/InDate64.h>

namespace normal::expression::gandiva {

InDate64::InDate64(const shared_ptr<Expression> &left, const unordered_set<int64_t> &values):
  In(left),
  values_(values) {}

void InDate64::compile(const shared_ptr<arrow::Schema> &schema) {
  left_->compile(schema);
  returnType_ = arrow::boolean();

  const auto &leftGandivaExpr = left_->getGandivaExpression();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionDate64(leftGandivaExpr, values_);
}

string InDate64::alias() {
  return "?column?";
}

string InDate64::getTypeString() {
  return "InDate64";
}

shared_ptr<Expression> inDate64_(const shared_ptr<Expression> &left, const unordered_set<int64_t> &values) {
  return make_shared<InDate64>(left, values);
}

}
