//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CAST_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CAST_H

#include <normal/expression/gandiva/Expression.h>

#include <arrow/api.h>
#include <gandiva/node.h>

#include <string>
#include <memory>

namespace normal::expression::gandiva {

class Cast : public Expression {

public:
  Cast(std::shared_ptr<Expression> expr, std::shared_ptr<arrow::DataType> type);

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

  std::shared_ptr<std::vector<std::string> > involvedColumnNames() override;


private:
  ::gandiva::NodePtr buildGandivaExpression();

private:
  std::shared_ptr<Expression> expr_;
  std::shared_ptr<arrow::DataType> type_;

};

std::shared_ptr<Expression> cast(const std::shared_ptr<Expression>& expr,
                                 const std::shared_ptr<arrow::DataType>& type);

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CAST_H
