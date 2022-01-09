//
// Created by Yifei Yang on 12/1/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSIONTYPE_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSIONTYPE_H

namespace normal::expression::gandiva {

enum ExpressionType {
  ADD,
  DATE_ADD,
  MULTIPLY,
  SUBTRACT,
  DIVIDE,
  AND,
  OR,
  NOT,
  CAST,
  COLUMN,
  EQUAL_TO,
  NOT_EQUAL_TO,
  GREATER_THAN,
  GREATER_THAN_OR_EQUAL_TO,
  LESS_THAN,
  LESS_THAN_OR_EQUAL_TO,
  NUMERIC_LITERAL,
  STRING_LITERAL,
  IN,
  IF,
  LIKE,
  DATE_EXTRACT,
  IS_NULL,
  SUBSTR
};

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSIONTYPE_H
