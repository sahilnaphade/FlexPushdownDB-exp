//
// Created by Yifei Yang on 11/8/21.
//

#include <normal/catalogue/Table.h>
#include <utility>

namespace normal::catalogue {

Table::Table(string name,
             const shared_ptr<arrow::Schema>& schema,
             const unordered_map<string, int> &apxColumnLengthMap,
             int apxRowLength,
             const unordered_set<string> &zonemapColumnNames,
             const shared_ptr<CatalogueEntry> &catalogueEntry) :
  name_(std::move(name)),
  schema_(schema),
  apxColumnLengthMap_(apxColumnLengthMap),
  apxRowLength_(apxRowLength),
  zonemapColumnNames_(zonemapColumnNames),
  catalogueEntry_(catalogueEntry) {}

const string &Table::getName() const {
  return name_;
}
}
