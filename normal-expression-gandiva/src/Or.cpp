//
// Created by Yifei Yang on 7/15/20.
//

#include <gandiva/tree_expr_builder.h>

#include <utility>
#include "normal/expression/gandiva/Or.h"

using namespace normal::expression::gandiva;

Or::Or(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
        : BinaryExpression(std::move(left), std::move(right), OR) {}

void Or::compile(std::shared_ptr<arrow::Schema> schema) {
  left_->compile(schema);
  right_->compile(schema);

  const auto &castRes = castGandivaExprToUpperType();
  const auto &leftGandivaExpr = get<1>(castRes);
  const auto &rightGandivaExpr = get<2>(castRes);

  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeOr({leftGandivaExpr, rightGandivaExpr});
}

std::string Or::alias() {
  return "(" + left_->alias() + " or " + right_->alias() + ")";
}

std::string Or::getTypeString() {
  return "Or";
}

std::shared_ptr<Expression> normal::expression::gandiva::or_(const std::shared_ptr<Expression>& left,
                                                              const std::shared_ptr<Expression>& right) {
  return std::make_shared<Or>(left, right);
}
