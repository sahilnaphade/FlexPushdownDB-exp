//
// Created by matt on 6/5/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LESSTHAN_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LESSTHAN_H

#include <string>
#include <memory>

#include "Expression.h"
#include "BinaryExpression.h"

namespace normal::expression::gandiva {

class LessThan : public BinaryExpression {

public:
  LessThan(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right);
  LessThan() = default;
  LessThan(const LessThan&) = default;
  LessThan& operator=(const LessThan&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LessThan& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("left", expr.left_),
                                 f.field("right", expr.right_));
  }
};

std::shared_ptr<Expression> lt(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right);

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LESSTHAN_H
