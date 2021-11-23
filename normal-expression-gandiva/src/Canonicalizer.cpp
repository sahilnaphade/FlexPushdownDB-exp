//
// Created by Yifei Yang on 11/22/21.
//

#include <normal/expression/gandiva/Canonicalizer.h>
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/Or.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/Util.h>

namespace normal::expression::gandiva {

shared_ptr<Expression> Canonicalizer::canonicalize(const shared_ptr<Expression> &expr) {
  auto type = expr->getType();
  switch (type) {
    case AND: {
      const auto &andExpr = static_pointer_cast<And>(expr);
      return and_(canonicalize(andExpr->getLeft()), andExpr->getRight());
    }

    case OR: {
      const auto &orExpr = static_pointer_cast<Or>(expr);
      return or_(canonicalize(orExpr->getLeft()), orExpr->getRight());
    }

    case EQUAL_TO:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL_TO:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL_TO: {
      const auto &biExpr = static_pointer_cast<BinaryExpression>(expr);
      const auto &leftExpr = biExpr->getLeft();
      const auto &rightExpr = biExpr->getRight();

      if (Util::isLiteral(leftExpr) && !Util::isLiteral(rightExpr)) {
        switch (type) {
          case EQUAL_TO: return eq(rightExpr, leftExpr);
          case LESS_THAN: return gt(rightExpr, leftExpr);
          case LESS_THAN_OR_EQUAL_TO: return gte(rightExpr, leftExpr);
          case GREATER_THAN: return lt(rightExpr, leftExpr);
          default: return lte(rightExpr, leftExpr);
        }
      }
    }

    default:
      return expr;
  };
}

}
