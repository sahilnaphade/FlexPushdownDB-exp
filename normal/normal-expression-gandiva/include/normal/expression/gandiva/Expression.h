//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H

#include <memory>

#include <arrow/type.h>
#include <gandiva/node.h>

#include <normal/expression/Expression.h>

namespace normal::expression::gandiva {

class Expression : public normal::expression::Expression {

public:
  virtual ~Expression() = default;

  [[nodiscard]] virtual std::string alias() = 0;

  [[nodiscard]] const ::gandiva::NodePtr &getGandivaExpression() const;

  std::string showString();

protected:
  ::gandiva::NodePtr gandivaExpression_;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H