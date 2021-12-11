//
// Created by Yifei Yang on 12/10/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_IN_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_IN_H

#include "Expression.h"

using namespace std;

namespace normal::expression::gandiva {

class In : public Expression {
public:
  In(const shared_ptr<Expression> &left);

  set<string> involvedColumnNames() override;

protected:
  shared_ptr<Expression> left_;
};

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_IN_H
