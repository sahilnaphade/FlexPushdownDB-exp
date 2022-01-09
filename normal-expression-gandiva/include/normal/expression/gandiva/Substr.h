//
// Created by Yifei Yang on 1/8/22.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_SUBSTR_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_SUBSTR_H

#include "Expression.h"
#include <memory>

using namespace std;

namespace normal::expression::gandiva {

class Substr : public Expression {

public:
  Substr(const shared_ptr<Expression> &expr,
         const shared_ptr<Expression> &fromLit,
         const shared_ptr<Expression> &forLit);

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() override;
  set<string> involvedColumnNames() override;

private:
  shared_ptr<Expression> expr_;
  shared_ptr<Expression> fromLit_;
  shared_ptr<Expression> forLit_;

};

shared_ptr<Expression> substr(const shared_ptr<Expression> &expr,
                              const shared_ptr<Expression> &fromLit,
                              const shared_ptr<Expression> &forLit);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_SUBSTR_H
