//
// Created by Yifei Yang on 12/11/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LIKE_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LIKE_H

#include "BinaryExpression.h"
#include <string>
#include <memory>

namespace normal::expression::gandiva {

class Like : public BinaryExpression {

public:
  Like(const shared_ptr<Expression>& left, const shared_ptr<Expression>& right);
  Like() = default;
  Like(const Like&) = default;
  Like& operator=(const Like&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Like& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("left", expr.left_),
                                 f.field("right", expr.right_));
  }
};

shared_ptr<Expression> like(const shared_ptr<Expression>& left, const shared_ptr<Expression>& right);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LIKE_H
