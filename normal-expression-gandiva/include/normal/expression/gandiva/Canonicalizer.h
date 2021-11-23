//
// Created by Yifei Yang on 11/22/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CANONICALIZER_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CANONICALIZER_H

#include <normal/expression/gandiva/Expression.h>

using namespace std;

namespace normal::expression::gandiva {

class Canonicalizer {

public:
  /**
   * Canonicalize expr so that for comparison, column is on the left and literal is on the right
   * @param expr
   * @return
   */
  static shared_ptr<Expression> canonicalize(const shared_ptr<Expression> &expr);

};

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CANONICALIZER_H
