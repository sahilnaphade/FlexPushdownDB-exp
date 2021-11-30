//
// Created by Yifei Yang on 11/22/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_UTIL_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_UTIL_H

#include <normal/expression/gandiva/Expression.h>

using namespace std;

namespace normal::expression::gandiva {

class Util {

public:
  static bool isLiteral(const shared_ptr<Expression> &expr);

};

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_UTIL_H
