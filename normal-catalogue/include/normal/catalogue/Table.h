//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_TABLE_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_TABLE_H

#include <normal/catalogue/CatalogueEntry.h>
#include <arrow/type.h>
#include <unordered_set>

using namespace std;

namespace normal::catalogue {

class CatalogueEntry;

class Table {
public:
  Table(string name,
        const shared_ptr<arrow::Schema>& schema,
        const unordered_map<string, int> &columnLengthMap,
        int rowLength,
        const unordered_set<string> &zoneMapColumnNames,
        const shared_ptr<CatalogueEntry> &catalogueEntry);

  const string &getName() const;

private:
  string name_;
  shared_ptr<arrow::Schema> schema_;
  unordered_map<string, int> columnLengthMap_;
  int rowLength_;
  unordered_set<string> zoneMapColumnNames_;
  shared_ptr<CatalogueEntry> catalogueEntry_;
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_TABLE_H
