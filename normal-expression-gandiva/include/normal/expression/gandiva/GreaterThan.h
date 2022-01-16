//
// Created by Yifei Yang on 7/14/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_GREATERTHAN_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_GREATERTHAN_H

#include <string>
#include <memory>

#include "Expression.h"
#include "BinaryExpression.h"

namespace normal::expression::gandiva {

class GreaterThan : public BinaryExpression {

public:
  GreaterThan(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right);
  GreaterThan() = default;
  GreaterThan(const GreaterThan&) = default;
  GreaterThan& operator=(const GreaterThan&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, GreaterThan& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("left", expr.left_),
                                 f.field("right", expr.right_));
  }
};

std::shared_ptr<Expression> gt(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_GREATERTHAN_H
