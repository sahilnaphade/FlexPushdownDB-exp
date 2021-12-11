//
// Created by Yifei Yang on 12/10/21.
//

#include <normal/expression/gandiva/In.h>
#include <fmt/format.h>
#include <sstream>

namespace normal::expression::gandiva {

// makeGandivaExpression()
template<>
void In<arrow::Int32Type, int32_t>::makeGandivaExpression() {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionInt32(left_->getGandivaExpression(), values_);
}

template<>
void In<arrow::Int64Type, int64_t>::makeGandivaExpression() {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionInt64(left_->getGandivaExpression(), values_);
}

template<>
void In<arrow::DoubleType, double>::makeGandivaExpression() {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionDouble(left_->getGandivaExpression(), values_);
}

template<>
void In<arrow::StringType, string>::makeGandivaExpression() {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionString(left_->getGandivaExpression(), values_);
}

template<>
void In<arrow::Date64Type, int64_t>::makeGandivaExpression() {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionDate64(left_->getGandivaExpression(), values_);
}


// alias()
template<>
string In<arrow::Int32Type, int32_t>::alias() {
  stringstream ss;
  const auto &leftAlias = fmt::format("cast({} as int)",left_->alias());
  uint i = 0;
  for (const auto &value: values_) {
    ss << fmt::format("({} = {})", leftAlias, value);
    if (i < values_.size() - 1) {
      ss << " or ";
    }
    ++i;
  }
  return ss.str();
}

template<>
string In<arrow::Int64Type, int64_t>::alias() {
  stringstream ss;
  const auto &leftAlias = fmt::format("cast({} as int)",left_->alias());
  uint i = 0;
  for (const auto &value: values_) {
    ss << fmt::format("({} = {})", leftAlias, value);
    if (i < values_.size() - 1) {
      ss << " or ";
    }
    ++i;
  }
  return ss.str();
}

template<>
string In<arrow::DoubleType, double>::alias() {
  stringstream ss;
  const auto &leftAlias = fmt::format("cast({} as float)",left_->alias());
  uint i = 0;
  for (const auto &value: values_) {
    ss << fmt::format("({} = {})", leftAlias, value);
    if (i < values_.size() - 1) {
      ss << " or ";
    }
    ++i;
  }
  return ss.str();
}

template<>
string In<arrow::StringType, string>::alias() {
  stringstream ss;
  const auto &leftAlias = left_->alias();
  uint i = 0;
  for (const auto &value: values_) {
    ss << fmt::format("({} = \'{}\')", leftAlias, value);
    if (i < values_.size() - 1) {
      ss << " or ";
    }
    ++i;
  }
  return ss.str();
}

template<>
string In<arrow::Date64Type, int64_t>::alias() {
  return "?column?";
}


// getTypeString()
template<>
string In<arrow::Int32Type, int32_t>::getTypeString() {
  return "InInt32";
}

template<>
string In<arrow::Int64Type, int64_t>::getTypeString() {
  return "InInt64";
}

template<>
string In<arrow::DoubleType, double>::getTypeString() {
  return "InDouble";
}

template<>
string In<arrow::StringType, string>::getTypeString() {
  return "InString";
}

template<>
string In<arrow::Date64Type, int64_t>::getTypeString() {
  return "InDate64";
}

}
