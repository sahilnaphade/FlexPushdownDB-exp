//
// Created by matt on 11/6/20.
//

#include "normal/expression/gandiva/EqualTo.h"

#include <utility>

#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

EqualTo::EqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
	: BinaryExpression(std::move(Left), std::move(Right), EQUAL_TO) {}

void EqualTo::compile(std::shared_ptr<arrow::Schema> Schema) {
  left_->compile(Schema);
  right_->compile(Schema);

  const auto &castRes = castGandivaExprToUpperType();
  const auto &leftGandivaExpr = get<1>(castRes);
  const auto &rightGandivaExpr = get<2>(castRes);

  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("equal",
                                                                {leftGandivaExpr, rightGandivaExpr},
                                                                returnType_);
}

std::string EqualTo::alias() {
  return genAliasForComparison("=");
}

std::string EqualTo::getTypeString() {
  return "EqualTo";
}

std::shared_ptr<Expression> normal::expression::gandiva::eq(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right) {
  return std::make_shared<EqualTo>(Left, Right);
}
