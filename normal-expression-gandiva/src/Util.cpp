//
// Created by Yifei Yang on 11/22/21.
//

#include <normal/expression/gandiva/Util.h>
#include <normal/expression/gandiva/Cast.h>

namespace normal::expression::gandiva {

bool Util::isLiteral(const shared_ptr <Expression> &expr) {
  return expr->getType() == NUMERIC_LITERAL || expr->getType() == STRING_LITERAL;
}

}
