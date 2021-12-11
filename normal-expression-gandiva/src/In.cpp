//
// Created by Yifei Yang on 12/10/21.
//

#include <normal/expression/gandiva/In.h>

namespace normal::expression::gandiva {

In::In(const shared_ptr<Expression> &left):
  Expression(IN),
  left_(left) {}

set<string> In::involvedColumnNames() {
  return left_->involvedColumnNames();
}

}
