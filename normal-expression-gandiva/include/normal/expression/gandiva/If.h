//
// Created by Yifei Yang on 12/10/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_IF_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_IF_H

#include "Expression.h"
#include <memory>

using namespace std;

namespace normal::expression::gandiva {

class If : public Expression {
public:
  If(const shared_ptr<Expression> &ifExpr,
     const shared_ptr<Expression> &thenExpr,
     const shared_ptr<Expression> &elseExpr);

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() override;
  set<string> involvedColumnNames() override;

private:
  shared_ptr<Expression> ifExpr_;
  shared_ptr<Expression> thenExpr_;
  shared_ptr<Expression> elseExpr_;

};

shared_ptr<Expression> if_(const shared_ptr<Expression> &ifExpr,
                           const shared_ptr<Expression> &thenExpr,
                           const shared_ptr<Expression> &elseExpr);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_IF_H
