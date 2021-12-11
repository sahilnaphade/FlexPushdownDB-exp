//
// Created by Yifei Yang on 12/10/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NOT_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NOT_H

#include "Expression.h"
#include <memory>

using namespace std;

namespace normal::expression::gandiva {

class Not : public Expression {

public:
  Not(const shared_ptr<Expression> &expr);

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() override;
  set<string> involvedColumnNames() override;

private:
  shared_ptr<Expression> expr_;

};

shared_ptr<Expression> not_(const shared_ptr<Expression> &expr);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NOT_H
