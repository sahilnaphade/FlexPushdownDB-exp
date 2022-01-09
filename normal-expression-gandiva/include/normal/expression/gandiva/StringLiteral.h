//
// Created by Yifei Yang on 7/15/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_STRINGLITERAL_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_STRINGLITERAL_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>

#include "Expression.h"

namespace normal::expression::gandiva {

class StringLiteral : public Expression {

public:
  explicit StringLiteral(std::optional<std::string> value);

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() override;
  std::set<std::string> involvedColumnNames() override;

  const std::optional<std::string> &value() const;

private:
  std::optional<std::string> value_;
};

std::shared_ptr<Expression> str_lit(const std::optional<std::string> &value);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_STRINGLITERAL_H
