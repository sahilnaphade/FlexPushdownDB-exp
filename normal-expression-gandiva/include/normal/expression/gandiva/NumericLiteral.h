//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NUMERICLITERAL_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NUMERICLITERAL_H

#include <string>
#include <memory>
#include <sstream>

#include <arrow/api.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>

#include "Expression.h"

namespace normal::expression::gandiva {

template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
class NumericLiteral : public Expression {

public:
  explicit NumericLiteral(C_TYPE value) :
    Expression(NUMERIC_LITERAL),
    value_(value) {}

  void compile(std::shared_ptr<arrow::Schema>) override {
    returnType_ = ::arrow::TypeTraits<ARROW_TYPE>::type_singleton();

    auto literal = ::gandiva::TreeExprBuilder::MakeLiteral(value_);
    // if the type is date64, the initial literal made from value_ will be int64, so we need to cast
    if (returnType_->id() == arrow::Type::DATE64) {
      literal = ::gandiva::TreeExprBuilder::MakeFunction("castDate", {literal}, returnType_);
    }

    gandivaExpression_ = literal;
  }

  std::string alias() override {
    if (typeid(ARROW_TYPE) == typeid(arrow::Int32Type) || typeid(ARROW_TYPE) == typeid(arrow::Int64Type)) {
      return prefixInt_ + std::to_string(value_);
    } else if (typeid(ARROW_TYPE) == typeid(arrow::DoubleType)) {
      return prefixFloat_ + std::to_string(value_);
    }
    throw std::runtime_error("Unsupported numeric literal type");
  }

  std::string getTypeString() override {
    std::stringstream ss;
    ss << "NumericLiteral";
    if (typeid(ARROW_TYPE) == typeid(arrow::Int32Type)) {
      ss << "<Int32>";
    } else if (typeid(ARROW_TYPE) == typeid(arrow::Int64Type)) {
      ss << "<Int64>";
    } else if (typeid(ARROW_TYPE) == typeid(arrow::DoubleType)) {
      ss << "<Double>";
    } else {
      throw std::runtime_error("Unsupported numeric literal type");
    }
    return ss.str();
  }

  std::set<std::string> involvedColumnNames() override{
    return std::set<std::string>();
  }

  C_TYPE value() const {
    return value_;
  }

private:
  C_TYPE value_;

};

template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
std::shared_ptr<Expression> num_lit(C_TYPE value){
  return std::make_shared<NumericLiteral<ARROW_TYPE>>(value);
}

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NUMERICLITERAL_H