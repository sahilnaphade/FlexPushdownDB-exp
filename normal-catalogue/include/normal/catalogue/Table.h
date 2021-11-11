//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_TABLE_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_TABLE_H

#include <normal/catalogue/CatalogueEntry.h>
#include <normal/catalogue/format/Format.h>
#include <arrow/type.h>
#include <unordered_set>

using namespace std;

namespace normal::catalogue {

class CatalogueEntry;

class Table {
public:
  Table(string name,
        const shared_ptr<arrow::Schema>& schema,
        const shared_ptr<format::Format>& format,
        const unordered_map<string, int> &apxColumnLengthMap,
        int apxRowLength,
        const unordered_set<string> &zonemapColumnNames,
        const shared_ptr<CatalogueEntry> &catalogueEntry);

  const string &getName() const;

private:
  string name_;
  shared_ptr<arrow::Schema> schema_;
  shared_ptr<format::Format> format_;
  unordered_map<string, int> apxColumnLengthMap_;   // apx: approximate
  int apxRowLength_;
  unordered_set<string> zonemapColumnNames_;
  shared_ptr<CatalogueEntry> catalogueEntry_;
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_TABLE_H
