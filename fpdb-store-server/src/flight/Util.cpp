//
// Created by Yifei Yang on 10/10/22.
//

#include "fpdb/store/server/flight/Util.hpp"

namespace fpdb::store::server::flight {

tl::expected<std::shared_ptr<arrow::Table>, std::string> Util::getEndTable() {
  auto schema = ::arrow::schema({{field(EndTableColumnName.data(), ::arrow::utf8())}});
  auto builder = std::make_shared<arrow::StringBuilder>();
  auto status = builder->Append(EndTableRowValue.data());
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  auto expArray = builder->Finish();
  if (!expArray.ok()) {
    return tl::make_unexpected(expArray.status().message());
  }
  return arrow::Table::Make(schema, arrow::ArrayVector{*expArray});
}

bool Util::isEndTable(const std::shared_ptr<arrow::Table> &table) {
  if (table == nullptr || table->num_rows() != 1 || table->num_columns() != 1) {
    return false;
  }
  auto column = table->GetColumnByName(EndTableColumnName.data());
  if (column == nullptr || column->type()->id() != arrow::StringType::type_id) {
    return false;
  }
  auto value = std::static_pointer_cast<arrow::StringArray>(column->chunk(0))->GetString(0);
  return value == EndTableRowValue.data();
}

}
