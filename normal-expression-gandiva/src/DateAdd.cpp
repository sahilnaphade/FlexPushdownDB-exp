//
// Created by Yifei Yang on 12/10/21.
//

#include <gandiva/tree_expr_builder.h>
#include <normal/expression/gandiva/DateAdd.h>
#include <sstream>

namespace normal::expression::gandiva {

DateAdd::DateAdd(const shared_ptr<Expression>& left, 
                 const shared_ptr<Expression>& right,
                 DateIntervalType intervalType):
  BinaryExpression(left, right, DATE_ADD),
  intervalType_(intervalType) {
}

void DateAdd::compile(shared_ptr<arrow::Schema> schema) {
  left_->compile(schema);
  right_->compile(schema);

  const auto &leftGandivaExpr = left_->getGandivaExpression();
  const auto &rightGandivaExpr = right_->getGandivaExpression();
  returnType_ = arrow::date64();

  switch (intervalType_) {
    case DAY: {
      gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("timestampaddDay",
                                                                    {leftGandivaExpr, rightGandivaExpr},
                                                                    returnType_);
      break;
    }
    case MONTH: {
      gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("timestampaddMonth",
                                                                    {leftGandivaExpr, rightGandivaExpr},
                                                                    returnType_);
      break;
    }
    case YEAR: {
      gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("timestampaddYear",
                                                                    {leftGandivaExpr, rightGandivaExpr},
                                                                    returnType_);
      break;
    }
    default:
      throw runtime_error("Unsupported date interval type");
  }
}

string DateAdd::alias() {
  return "?column?";
}

string DateAdd::getTypeString() {
  stringstream ss;
  ss << "DateAdd-";
  switch (intervalType_) {
    case DAY: {
      ss << "Day";
      break;
    }
    case MONTH: {
      ss << "Month";
      break;
    }
    case YEAR: {
      ss << "Year";
      break;
    }
    default:
      throw runtime_error("Unsupported date interval type");
  }
  return ss.str();
}

shared_ptr<Expression> datePlus(const shared_ptr<Expression>& left,
                                const shared_ptr<Expression>& right,
                                DateIntervalType intervalType) {
  return make_shared<DateAdd>(left, right, intervalType);
}

}
