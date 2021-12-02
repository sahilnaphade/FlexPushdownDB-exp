//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H

#include <normal/expression/Expression.h>
#include <normal/expression/gandiva/ExpressionType.h>
#include <arrow/type.h>
#include <gandiva/node.h>
#include <memory>
#include <set>

namespace normal::expression::gandiva {

class Expression : public normal::expression::Expression {

public:
  Expression(ExpressionType type);

  virtual ~Expression() = default;

  virtual std::string alias() = 0;

  virtual std::set<std::string> involvedColumnNames() = 0;

  virtual std::string getTypeString() = 0;

  ExpressionType getType() const;
  const ::gandiva::NodePtr &getGandivaExpression() const;

  std::string showString();

protected:
  ExpressionType type_;
  ::gandiva::NodePtr gandivaExpression_;

};

const inline std::string prefixInt_ = "int:";
const inline std::string prefixFloat_ = "float:";
std::shared_ptr<std::string> removePrefixInt(const std::string&);
std::shared_ptr<std::string> removePrefixFloat(const std::string&);

std::shared_ptr<normal::expression::gandiva::Expression>
cascadeCast(std::shared_ptr<normal::expression::gandiva::Expression> expr);

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H
