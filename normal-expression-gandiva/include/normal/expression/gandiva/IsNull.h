//
// Created by Yifei Yang on 1/7/22.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_ISNULL_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_ISNULL_H

#include "Expression.h"
#include <memory>

using namespace std;

namespace normal::expression::gandiva {

class IsNull : public Expression {

public:
  IsNull(const shared_ptr<Expression> &expr);
  IsNull() = default;
  IsNull(const IsNull&) = default;
  IsNull& operator=(const IsNull&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() override;
  set<string> involvedColumnNames() override;

private:
  shared_ptr<Expression> expr_;

};

shared_ptr<Expression> isNull(const shared_ptr<Expression> &expr);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_ISNULL_H
