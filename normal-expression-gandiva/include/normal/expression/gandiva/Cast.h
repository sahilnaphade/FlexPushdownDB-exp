//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CAST_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CAST_H

#include <normal/expression/gandiva/Expression.h>
#include <normal/tuple/DataType.h>
#include <arrow/api.h>
#include <gandiva/node.h>
#include <string>
#include <memory>

namespace normal::expression::gandiva {

class Cast : public Expression {

public:
  Cast(std::shared_ptr<Expression> expr, std::shared_ptr<arrow::DataType> dataType);
  Cast() = default;
  Cast(const Cast&) = default;
  Cast& operator=(const Cast&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() override;
  std::set<std::string> involvedColumnNames() override;

  const std::shared_ptr<Expression> &getExpr() const;


private:
  ::gandiva::NodePtr buildGandivaExpression();

  std::shared_ptr<Expression> expr_;
  //use normal::tuple::DataType instead of arrow::DataType for ease of serialization
  std::shared_ptr<arrow::DataType> dataType_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Cast& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("expr", expr.expr_),
                                 f.field("dataType", expr.dataType_));
  }
};

std::shared_ptr<Expression> cast(const std::shared_ptr<Expression>& expr,
                                 const std::shared_ptr<arrow::DataType>& type);

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CAST_H
