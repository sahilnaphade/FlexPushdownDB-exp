//
// Created by Yifei Yang on 12/1/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSIONTYPE_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSIONTYPE_H

namespace normal::expression::gandiva {

enum ExpressionType {
  ADD,
  MULTIPLY,
  SUBTRACT,
  DIVIDE,
  AND,
  OR,
  CAST,
  COLUMN,
  EQUAL_TO,
  GREATER_THAN,
  GREATER_THAN_OR_EQUAL_TO,
  LESS_THAN,
  LESS_THAN_OR_EQUAL_TO,
  NUMERIC_LITERAL,
  STRING_LITERAL
};

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSIONTYPE_H
