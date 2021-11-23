//
// Created by matt on 27/4/20.
//

#include <normal/expression/gandiva/Expression.h>
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/Or.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <normal/util/Util.h>

using namespace normal::expression::gandiva;
using namespace normal::util;

Expression::Expression(ExpressionType type) :
  type_(type) {}

ExpressionType Expression::getType() const {
  return type_;
}

const gandiva::NodePtr &Expression::getGandivaExpression() const {
  return gandivaExpression_;
}

std::string Expression::showString() {
  return gandivaExpression_->ToString();
}

std::shared_ptr<std::string> normal::expression::gandiva::removePrefixInt(const std::string& str) {
  if (str.substr(0, prefixInt_.size()) == prefixInt_) {
    return std::make_shared<std::string>(str.substr(prefixInt_.size(), str.size() - prefixInt_.size()));
  } else {
    return nullptr;
  }
}

std::shared_ptr<std::string> normal::expression::gandiva::removePrefixFloat(const std::string& str) {
  if (str.substr(0, prefixFloat_.size()) == prefixFloat_) {
    return std::make_shared<std::string>(str.substr(prefixFloat_.size(), str.size() - prefixFloat_.size()));
  } else {
    return nullptr;
  }
}

std::shared_ptr<arrow::DataType> getNumericType(const std::shared_ptr<normal::expression::gandiva::Expression>& expr) {
  if (instanceof<normal::expression::gandiva::NumericLiteral<arrow::Int32Type>>(expr)) {
    return arrow::int32();
  } else if (instanceof<normal::expression::gandiva::NumericLiteral<arrow::Int64Type>>(expr)) {
    return arrow::int64();
  } else if (instanceof<normal::expression::gandiva::NumericLiteral<arrow::DoubleType>>(expr)) {
    return arrow::float64();
  } else {
    return nullptr;
  }
}

std::shared_ptr<normal::expression::gandiva::Expression> 
normal::expression::gandiva::cascadeCast(std::shared_ptr<normal::expression::gandiva::Expression> expr) {
  if (expr->getType() == AND) {
    auto andExpr = std::static_pointer_cast<normal::expression::gandiva::And>(expr);
    return and_(cascadeCast(andExpr->getLeft()), cascadeCast(andExpr->getRight()));
  }

  else if (expr->getType() == OR) {
    auto orExpr = std::static_pointer_cast<normal::expression::gandiva::Or>(expr);
    return or_(cascadeCast(orExpr->getLeft()), cascadeCast(orExpr->getRight()));
  }

  else if (expr->getType() == EQUAL_TO) {
    auto eqExpr = std::static_pointer_cast<normal::expression::gandiva::EqualTo>(expr);
    auto leftExpr = eqExpr->getLeft();
    auto rightExpr = eqExpr->getRight();
    auto leftType = getNumericType(leftExpr);
    if (leftType) {
      return eq(leftExpr, normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getNumericType(rightExpr);
      if (rightType) {
        return eq(normal::expression::gandiva::cast(leftExpr, rightType), rightExpr);
      }
    }
    return expr;
  }

  else if (expr->getType() == GREATER_THAN) {
    auto gtExpr = std::static_pointer_cast<normal::expression::gandiva::GreaterThan>(expr);
    auto leftExpr = gtExpr->getLeft();
    auto rightExpr = gtExpr->getRight();
    auto leftType = getNumericType(leftExpr);
    if (leftType) {
      return gt(leftExpr, normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getNumericType(rightExpr);
      if (rightType) {
        return gt(normal::expression::gandiva::cast(leftExpr, rightType), rightExpr);
      }
    }
    return expr;
  }

  else if (expr->getType() == GREATER_THAN_OR_EQUAL_TO) {
    auto geExpr = std::static_pointer_cast<normal::expression::gandiva::GreaterThanOrEqualTo>(expr);
    auto leftExpr = geExpr->getLeft();
    auto rightExpr = geExpr->getRight();
    auto leftType = getNumericType(leftExpr);
    if (leftType) {
      return gte(leftExpr, normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getNumericType(rightExpr);
      if (rightType) {
        return gte(normal::expression::gandiva::cast(leftExpr, rightType), rightExpr);
      }
    }
    return expr;
  }

  else if (expr->getType() == LESS_THAN) {
    auto ltExpr = std::static_pointer_cast<normal::expression::gandiva::LessThan>(expr);
    auto leftExpr = ltExpr->getLeft();
    auto rightExpr = ltExpr->getRight();
    auto leftType = getNumericType(leftExpr);
    if (leftType) {
      return lt(leftExpr, normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getNumericType(rightExpr);
      if (rightType) {
        return lt(normal::expression::gandiva::cast(leftExpr, rightType), rightExpr);
      }
    }
    return expr;
  }

  else if (expr->getType() == LESS_THAN_OR_EQUAL_TO) {
    auto leExpr = std::static_pointer_cast<normal::expression::gandiva::LessThanOrEqualTo>(expr);
    auto leftExpr = leExpr->getLeft();
    auto rightExpr = leExpr->getRight();
    auto leftType = getNumericType(leftExpr);
    if (leftType) {
      return lte(leftExpr, normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getNumericType(rightExpr);
      if (rightType) {
        return lte(normal::expression::gandiva::cast(leftExpr, rightType), rightExpr);
      }
    }
    return expr;
  }

  return expr;
}
