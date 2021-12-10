//
// Created by matt on 11/6/20.
//

#include <gandiva/tree_expr_builder.h>
#include "normal/expression/gandiva/And.h"

using namespace normal::expression::gandiva;

And::And(const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right)
	: BinaryExpression(left, right, AND) {
}

void And::compile(std::shared_ptr<arrow::Schema> schema) {
  left_->compile(schema);
  right_->compile(schema);

  const auto &castRes = castGandivaExprToUpperType();
  const auto &leftGandivaExpr = get<1>(castRes);
  const auto &rightGandivaExpr = get<2>(castRes);

  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeAnd({leftGandivaExpr, rightGandivaExpr});
}

std::string And::alias() {
  return "(" + left_->alias() + " and " + right_->alias() + ")";
}

std::string And::getTypeString() {
  return "And";
}

std::shared_ptr<Expression> normal::expression::gandiva::and_(const std::shared_ptr<Expression>& left,
															  const std::shared_ptr<Expression>& right) {
  return std::make_shared<And>(left, right);
}

std::shared_ptr<Expression>
normal::expression::gandiva::and_(const std::vector<std::shared_ptr<Expression>> &operands) {
  if (operands.empty()) {
    return nullptr;
  } else if (operands.size() == 1) {
    return operands[0];
  } else {
    auto left = operands[0];
    for (uint i = 1; i < operands.size(); ++i) {
      const auto &right = operands[i];
      left = std::make_shared<And>(left, right);
    }
    return left;
  }
}
