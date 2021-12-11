//
// Created by Yifei Yang on 12/10/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_IN_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_IN_H

#include "Expression.h"
#include <gandiva/tree_expr_builder.h>
#include <unordered_set>

using namespace std;

namespace normal::expression::gandiva {

template<typename ARROW_TYPE, typename C_TYPE>
class In : public Expression {

public:
  In(const shared_ptr<Expression> &left, const unordered_set<C_TYPE> &values):
    Expression(IN),
    left_(left),
    values_(values) {}

  void compile(const shared_ptr<arrow::Schema> &schema) override {
    left_->compile(schema);
    returnType_ = arrow::boolean();
    makeGandivaExpression();
  }

  void makeGandivaExpression();

  string alias() override;

  string getTypeString() override;

  set<string> involvedColumnNames() override {
    return left_->involvedColumnNames();
  }

private:
  shared_ptr<Expression> left_;
  unordered_set<C_TYPE> values_;
};

template<typename ARROW_TYPE, typename C_TYPE>
shared_ptr<Expression> in(const shared_ptr<Expression> &left, const unordered_set<C_TYPE> &values) {
  return make_shared<In<ARROW_TYPE, C_TYPE>>(left, values);
}

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_IN_H
