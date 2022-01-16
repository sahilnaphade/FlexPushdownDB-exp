//
// Created by Yifei Yang on 12/16/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_DATEEXTRACT_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_DATEEXTRACT_H

#include "Expression.h"
#include "DateIntervalType.h"
#include <string>
#include <memory>

using namespace std;

namespace normal::expression::gandiva {

class DateExtract : public Expression {

public:
  DateExtract(const shared_ptr<Expression> &dateExpr, DateIntervalType intervalType);
  DateExtract() = default;
  DateExtract(const DateExtract&) = default;
  DateExtract& operator=(const DateExtract&) = default;

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() override;
  set<string> involvedColumnNames() override;

private:
  shared_ptr<Expression> dateExpr_;
  DateIntervalType intervalType_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DateExtract& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("dateExpr", expr.dateExpr_),
                                 f.field("intervalType", expr.intervalType_));
  }
};

shared_ptr<Expression> dateExtract(const shared_ptr<Expression> &dateExpr, DateIntervalType intervalType);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_DATEEXTRACT_H
