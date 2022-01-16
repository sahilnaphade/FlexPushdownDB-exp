//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_SUBTRACT_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_SUBTRACT_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>

#include "Expression.h"
#include "BinaryExpression.h"

namespace normal::expression::gandiva {

class Subtract : public BinaryExpression {

public:
  Subtract(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);
  Subtract() = default;
  Subtract(const Subtract&) = default;
  Subtract& operator=(const Subtract&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() override;

};

std::shared_ptr<Expression> minus(const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right);

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_SUBTRACT_H
