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
#include "BinaryExpression.h"

namespace normal::expression::gandiva {

class And : public BinaryExpression {

public:
  And(const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right);

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() override;

};

std::shared_ptr<Expression> and_(const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right);
std::shared_ptr<Expression> and_(const std::vector<std::shared_ptr<Expression>> &operands);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_AND_H
