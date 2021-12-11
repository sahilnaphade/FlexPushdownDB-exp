//
// Created by Yifei Yang on 7/14/20.
//

#include "normal/expression/gandiva/GreaterThan.h"

#include <utility>

#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

GreaterThan::GreaterThan(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
  : BinaryExpression(std::move(Left), std::move(Right), GREATER_THAN) {}

void GreaterThan::compile(const std::shared_ptr<arrow::Schema> &schema) {
  left_->compile(schema);
  right_->compile(schema);

  const auto &castRes = castGandivaExprToUpperType();
  const auto &leftGandivaExpr = get<1>(castRes);
  const auto &rightGandivaExpr = get<2>(castRes);

  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("greater_than",
                                                                {leftGandivaExpr, rightGandivaExpr},
                                                                returnType_);
}

std::string normal::expression::gandiva::GreaterThan::alias() {
  return genAliasForComparison(">");
}

std::string GreaterThan::getTypeString() {
  return "GreaterThan";
}

std::shared_ptr<Expression> normal::expression::gandiva::gt(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right) {
  return std::make_shared<GreaterThan>(Left, Right);
}
