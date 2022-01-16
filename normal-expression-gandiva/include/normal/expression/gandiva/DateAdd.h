//
// Created by Yifei Yang on 12/10/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_DATEADD_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_DATEADD_H

#include "Expression.h"
#include "BinaryExpression.h"
#include "DateIntervalType.h"

using namespace std;

/**
 * Currently gandiva does not support date diff, so we transfer it to date add
 */
namespace normal::expression::gandiva {

class DateAdd : public BinaryExpression {

public:
  DateAdd(const shared_ptr<Expression>& left,
          const shared_ptr<Expression>& right,
          DateIntervalType intervalType);
  DateAdd() = default;
  DateAdd(const DateAdd&) = default;
  DateAdd& operator=(const DateAdd&) = default;

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() override;

private:
  DateIntervalType intervalType_;

};

shared_ptr<Expression> datePlus(const shared_ptr<Expression>& left,
                                const shared_ptr<Expression>& right,
                                DateIntervalType intervalType);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_DATEADD_H
