//
// Created by matt on 27/4/20.
//

#include "normal/expression/gandiva/Column.h"

#include <gandiva/tree_expr_builder.h>

#include <normal/tuple/ColumnName.h>

using namespace normal::expression::gandiva;

Column::Column(std::string columnName):
  Expression(COLUMN),
  columnName_(std::move(columnName)) {}

void Column::compile(const std::shared_ptr<arrow::Schema> &schema) {
  auto field = schema->GetFieldByName(columnName_);
  if(field == nullptr){
    throw std::runtime_error("Column '" + columnName_ + "' does not exist");
  }
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeField(field);
  returnType_ = field->type();
}

std::string Column::alias() {
  return columnName_;
}

const std::string &Column::getColumnName() const {
  return columnName_;
}

std::set<std::string> Column::involvedColumnNames() {
  std::set<std::string> involvedColumnNames;
  involvedColumnNames.insert(columnName_);
  return involvedColumnNames;
}

std::string Column::getTypeString() {
  return "Column";
}

std::shared_ptr<Expression> normal::expression::gandiva::col(const std::string& columnName) {
  auto canonicalColumnName = normal::tuple::ColumnName::canonicalize(columnName);
  return std::make_shared<Column>(canonicalColumnName);
}
