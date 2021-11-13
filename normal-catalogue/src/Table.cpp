//
// Created by Yifei Yang on 11/8/21.
//

#include <normal/catalogue/Table.h>
#include <utility>

namespace normal::catalogue {

Table::Table(string name,
             const shared_ptr<arrow::Schema>& schema,
             const shared_ptr<format::Format>& format,
             const unordered_map<string, int> &apxColumnLengthMap,
             int apxRowLength,
             const unordered_set<string> &zonemapColumnNames,
             const shared_ptr<CatalogueEntry> &catalogueEntry) :
  name_(std::move(name)),
  schema_(schema),
  format_(format),
  apxColumnLengthMap_(apxColumnLengthMap),
  apxRowLength_(apxRowLength),
  zonemapColumnNames_(zonemapColumnNames),
  catalogueEntry_(catalogueEntry) {}

const string &Table::getName() const {
  return name_;
}

const vector<string> Table::getColumnNames() const {
  return schema_->field_names();
}

}
