//
// Created by Yifei Yang on 1/6/22.
//

#include <normal/expression/gandiva/NotEqualTo.h>
#include <gandiva/selection_vector.h>
#include <gandiva/tree_expr_builder.h>
#include <utility>

namespace normal::expression::gandiva {

NotEqualTo::NotEqualTo(shared_ptr<Expression> Left, shared_ptr<Expression> Right): 
  BinaryExpression(move(Left), move(Right), NOT_EQUAL_TO) {}

void NotEqualTo::compile(const shared_ptr<arrow::Schema> &schema) {
  left_->compile(schema);
  right_->compile(schema);

  const auto &castRes = castGandivaExprToUpperType();
  const auto &leftGandivaExpr = get<1>(castRes);
  const auto &rightGandivaExpr = get<2>(castRes);

  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("not_equal",
                                                                {leftGandivaExpr, rightGandivaExpr},
                                                                returnType_);
}
  
string NotEqualTo::alias() {
  return genAliasForComparison("<>");
}

string NotEqualTo::getTypeString() {
  return "NotEqualTo";
}

shared_ptr<Expression> neq(const shared_ptr<Expression>& left, const shared_ptr<Expression>& right) {
  return make_shared<NotEqualTo>(left, right);
}
          
}
