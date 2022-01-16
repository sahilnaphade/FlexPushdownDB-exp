//
// Created by matt on 28/4/20.
//

#include <normal/expression/gandiva/NumericLiteral.h>

namespace normal::expression::gandiva {

template<>
string NumericLiteral<arrow::Int32Type>::getTypeString() {
  return "NumericLiteral<Int32>";
}

template<>
string NumericLiteral<arrow::Int64Type>::getTypeString() {
  return "NumericLiteral<Int64>";
}

template<>
string NumericLiteral<arrow::DoubleType>::getTypeString() {
  return "NumericLiteral<Double>";
}

template<>
string NumericLiteral<arrow::BooleanType>::getTypeString() {
  return "NumericLiteral<Boolean>";
}

template<>
string NumericLiteral<arrow::Date64Type>::getTypeString() {
  return "NumericLiteral<Date64>";
}

}
