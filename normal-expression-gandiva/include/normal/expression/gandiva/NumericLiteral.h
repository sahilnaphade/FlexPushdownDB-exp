//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NUMERICLITERAL_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NUMERICLITERAL_H

#include "Expression.h"
#include "DateIntervalType.h"
#include <arrow/api.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>
#include <fmt/format.h>
#include <string>
#include <memory>
#include <sstream>

namespace normal::expression::gandiva {

template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
class NumericLiteral : public Expression {

public:
  explicit NumericLiteral(std::optional<C_TYPE> value, std::optional<DateIntervalType> intervalType) :
    Expression(NUMERIC_LITERAL),
    value_(value),
    intervalType_(intervalType) {}
  NumericLiteral() = default;
  NumericLiteral(const NumericLiteral&) = default;
  NumericLiteral& operator=(const NumericLiteral&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &) override {
    returnType_ = ::arrow::TypeTraits<ARROW_TYPE>::type_singleton();

    ::gandiva::NodePtr literal;
    if (value_.has_value()) {
      literal = ::gandiva::TreeExprBuilder::MakeLiteral(*value_);
    } else {
      literal = ::gandiva::TreeExprBuilder::MakeNull(returnType_);
    }
    // if the type is date64, the initial literal made from value_ will be int64, so we need to cast
    if (returnType_->id() == arrow::Type::DATE64) {
      literal = ::gandiva::TreeExprBuilder::MakeFunction("castDate", {literal}, returnType_);
    }

    gandivaExpression_ = literal;
  }

  std::string alias() override {
    const shared_ptr<arrow::DataType> &arrowType = ::arrow::TypeTraits<ARROW_TYPE>::type_singleton();
    if (arrowType->id() == arrow::Type::INT32 || arrowType->id() == arrow::Type::INT64) {
      if (value_.has_value()) {
        return prefixInt_ + std::to_string(*value_);
      } else {
        return prefixInt_ + "0";
      }
    } else if (arrowType->id() == arrow::Type::DOUBLE) {
      if (value_.has_value()) {
        return prefixFloat_ + std::to_string(*value_);
      } else {
        return prefixFloat_ + "0.0";
      };
    } else {
      if (value_.has_value()) {
        return std::to_string(*value_);
      } else {
        return "0";
      }
    }
  }

  std::string getTypeString() override;

  std::set<std::string> involvedColumnNames() override{
    return {};
  }

  std::optional<C_TYPE> value() const {
    return value_;
  }

  const optional<DateIntervalType> &getIntervalType() const {
    return intervalType_;
  }

  // make the opposite literal of the current one, e.g. 5 -> -5
  void makeOpposite() {
    if (value_.has_value()) {
      value_ = std::optional<C_TYPE>(-*value_);
    } else {
      throw runtime_error("Cannot make opposite on null literal");
    }
  }

private:
  std::optional<C_TYPE> value_;
  std::optional<DateIntervalType> intervalType_;    // denote whether this literal is used as an interval

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, NumericLiteral& expr) {
    return f.object(exp).fields(f.field("type", expr.type_),
                                f.field("value", expr.value_),
                                f.field("intervalType", expr.intervalType_));
  }
};

template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
std::shared_ptr<Expression> num_lit(std::optional<C_TYPE> value,
                                    std::optional<DateIntervalType> intervalType = std::nullopt){
  return std::make_shared<NumericLiteral<ARROW_TYPE>>(value, intervalType);
}

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NUMERICLITERAL_H
