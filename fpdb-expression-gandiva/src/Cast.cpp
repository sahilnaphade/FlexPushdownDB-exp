//
// Created by matt on 27/4/20.
//

#include "fpdb/expression/gandiva/Cast.h"
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <fmt/format.h>
#include <utility>

#include <gandiva/tree_expr_builder.h>

using namespace fpdb::expression::gandiva;

Cast::Cast(std::shared_ptr<Expression> expr, std::shared_ptr<arrow::DataType> dataType) :
  Expression(CAST),
	expr_(std::move(expr)), dataType_(std::move(dataType)) {}

::gandiva::NodePtr Cast::buildGandivaExpression() {

  auto paramGandivaExpression = expr_->getGandivaExpression();
  auto fromArrowType = expr_->getReturnType();

  /**
   * NOTE: Some cast operations are not supported by Gandiva so we set up some special cases here
   */
  if (fromArrowType->id() == arrow::utf8()->id() && dataType_->id() == arrow::float64()->id()) {
	// Not supported directly by Gandiva, need to cast string to decimal and then that to float64

	auto castDecimalFunctionName = "castDECIMAL";
	auto castDecimalReturnType = arrow::decimal(30, 8); // FIXME: Need to check if this is sufficient to cast to double
	auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
																		  {paramGandivaExpression},
																		  castDecimalReturnType);

	auto castFunctionName = "castFloat8";

	auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
																 {castToDecimalFunction},
                                 dataType_);

	return castFunction;
  } else if (fromArrowType->id() == arrow::utf8()->id() && dataType_->id() == arrow::int64()->id()) {
	// Not supported directly by Gandiva, need to cast string to decimal and then that to int64

	auto castDecimalFunctionName = "castDECIMAL";
	auto castDecimalReturnType = arrow::decimal(38, 0); // FIXME: Need to check if this is sufficient to cast to double
	auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
																		  {paramGandivaExpression},
																		  castDecimalReturnType);

	auto castFunctionName = "castBIGINT";

	auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
																 {castToDecimalFunction},
                                 dataType_);

	return castFunction;
  }
  else if (fromArrowType->id() == arrow::utf8()->id() && dataType_->id() == arrow::int32()->id()) {
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
                                 dataType_);

	return castFunction;
  } else {

	auto function = "castDECIMAL";

	auto expressionNode = ::gandiva::TreeExprBuilder::MakeFunction(function,
                                                                 {paramGandivaExpression},
                                                                 dataType_);

	return expressionNode;
  }
}

void Cast::compile(const std::shared_ptr<arrow::Schema> &schema) {
  expr_->compile(schema);

  gandivaExpression_ = buildGandivaExpression();
  returnType_ = dataType_;
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

std::string Cast::getTypeString() const {
  return "Cast";
}

::nlohmann::json Cast::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());
  jObj.emplace("expr", expr_->toJson());
  jObj.emplace("dataType", fpdb::tuple::ArrowSerializer::dataType_to_bytes(dataType_));
  return jObj;
}

tl::expected<std::shared_ptr<Cast>, std::string> Cast::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("expr")) {
    return tl::make_unexpected(fmt::format("Expr not specified in Cast expression JSON '{}'", jObj));
  }
  auto expExpr = Expression::fromJson(jObj["expr"]);
  if (!expExpr.has_value()) {
    return tl::make_unexpected(expExpr.error());
  }

  if (!jObj.contains("dataType")) {
    return tl::make_unexpected(fmt::format("DataType not specified in Cast expression JSON '{}'", jObj));
  }
  auto dataType = fpdb::tuple::ArrowSerializer::bytes_to_dataType(jObj["dataType"].get<std::vector<uint8_t>>());

  return std::make_shared<Cast>(*expExpr, dataType);
}

std::shared_ptr<Expression> fpdb::expression::gandiva::cast(const std::shared_ptr<Expression>& expr,
                                                              const std::shared_ptr<arrow::DataType> &type) {
  return std::make_shared<Cast>(expr, type);
}
