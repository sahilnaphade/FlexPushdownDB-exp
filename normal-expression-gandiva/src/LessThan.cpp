//
// Created by matt on 6/5/20.
//

#include "normal/expression/gandiva/LessThan.h"

#include <utility>

#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

LessThan::LessThan(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
	: BinaryExpression(std::move(Left), std::move(Right), LESS_THAN) {}

void LessThan::compile(const std::shared_ptr<arrow::Schema> &Schema) {
  left_->compile(Schema);
  right_->compile(Schema);

  const auto &castRes = castGandivaExprToUpperType();
  const auto &leftGandivaExpr = get<1>(castRes);
  const auto &rightGandivaExpr = get<2>(castRes);

  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("less_than",
                                                                {leftGandivaExpr, rightGandivaExpr},
                                                                returnType_);
}

std::string LessThan::alias() {
  return genAliasForComparison("<");
}

std::string LessThan::getTypeString() {
  return "LessThan";
}

std::shared_ptr<Expression> normal::expression::gandiva::lt(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right) {
  return std::make_shared<LessThan>(Left, Right);
}
