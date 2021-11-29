//
// Created by matt on 27/4/20.
//

#include "normal/expression/gandiva/Cast.h"

#include <utility>

#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

Cast::Cast(std::shared_ptr<Expression> expr, std::shared_ptr<arrow::DataType> type) :
  Expression(CAST),
	expr_(std::move(expr)), type_(std::move(type)) {}

::gandiva::NodePtr Cast::buildGandivaExpression() {

  auto paramGandivaExpression = expr_->getGandivaExpression();
  auto fromArrowType = expr_->getReturnType();

  /**
   * NOTE: Some cast operations are not supported by Gandiva so we set up some special cases here
   */
  if (fromArrowType->id() == arrow::utf8()->id() && type_->id() == arrow::float64()->id()) {
	// Not supported directly by Gandiva, need to cast string to decimal and then that to float64

	auto castDecimalFunctionName = "castDECIMAL";
	auto castDecimalReturnType = arrow::decimal(30, 8); // FIXME: Need to check if this is sufficient to cast to double
	auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
																		  {paramGandivaExpression},
																		  castDecimalReturnType);

	auto castFunctionName = "castFloat8";

	auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
																 {castToDecimalFunction},
																 type_);

	return castFunction;
  } else if (fromArrowType->id() == arrow::utf8()->id() && type_->id() == arrow::int64()->id()) {
	// Not supported directly by Gandiva, need to cast string to decimal and then that to int64

	auto castDecimalFunctionName = "castDECIMAL";
	auto castDecimalReturnType = arrow::decimal(38, 0); // FIXME: Need to check if this is sufficient to cast to double
	auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
																		  {paramGandivaExpression},
																		  castDecimalReturnType);

	auto castFunctionName = "castBIGINT";

	auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
																 {castToDecimalFunction},
																 type_);

	return castFunction;
  }
  else if (fromArrowType->id() == arrow::utf8()->id() && type_->id() == arrow::int32()->id()) {
	// Not supported directly by Gandiva, need to cast string to decimal to int64 and then that to int32

	auto castDecimalFunctionName = "castDECIMAL";
	auto castDecimalReturnType = arrow::decimal(38, 0); // FIXME: Need to check if this is sufficient to cast to double
	auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
																		  {paramGandivaExpression},
																		  castDecimalReturnType);

	auto castToInt64Function = ::gandiva::TreeExprBuilder::MakeFunction("castBIGINT",
																		  {castToDecimalFunction},
																		  ::arrow::int64());

	auto castFunctionName = "castINT";

	auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
																 {castToInt64Function},
																 type_);

	return castFunction;
  } else {

	auto function = "castDECIMAL";

	auto expressionNode = ::gandiva::TreeExprBuilder::MakeFunction(function,
                                                                 {paramGandivaExpression},
                                                                 type_);

	return expressionNode;
  }
}

void Cast::compile(std::shared_ptr<arrow::Schema> schema) {
  expr_->compile(schema);

  gandivaExpression_ = buildGandivaExpression();
  returnType_ = type_;
}

std::string Cast::alias() {
  return expr_->alias();
}

std::set<std::string> Cast::involvedColumnNames() {
  return expr_->involvedColumnNames();
}

const std::shared_ptr<Expression> &Cast::getExpr() const {
  return expr_;
}

std::string Cast::getTypeString() {
  return "Cast";
}

std::shared_ptr<Expression> normal::expression::gandiva::cast(const std::shared_ptr<Expression>& expr,
                                                              const std::shared_ptr<arrow::DataType> &type) {
  return std::make_shared<Cast>(expr, type);
}
