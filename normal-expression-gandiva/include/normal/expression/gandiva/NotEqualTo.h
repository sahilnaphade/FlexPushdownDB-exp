//
// Created by Yifei Yang on 1/6/22.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NOTEQUALTO_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NOTEQUALTO_H

#include <string>
#include <memory>

#include "Expression.h"
#include "BinaryExpression.h"

namespace normal::expression::gandiva {

class NotEqualTo : public BinaryExpression {

public:
  NotEqualTo(shared_ptr<Expression> Left, shared_ptr<Expression> Right);
  NotEqualTo() = default;
  NotEqualTo(const NotEqualTo&) = default;
  NotEqualTo& operator=(const NotEqualTo&) = default;

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() override;
  
};

shared_ptr<Expression> neq(const shared_ptr<Expression>& left, const shared_ptr<Expression>& right);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NOTEQUALTO_H
