//
// Created by matt on 11/6/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_AND_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_AND_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>

#include "Expression.h"

using namespace std;

namespace normal::expression::gandiva {

class And : public Expression {

public:
  And(const vector<shared_ptr<Expression>>& exprs);

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() override;
  set<string> involvedColumnNames() override;

  const vector<shared_ptr<Expression>> &getExprs() const;

private:
  vector<shared_ptr<Expression>> exprs_;

};

shared_ptr<Expression> and_(const shared_ptr<Expression>& left, const shared_ptr<Expression>& right);
shared_ptr<Expression> and_(const vector<shared_ptr<Expression>> &exprs);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_AND_H
