//
// Created by Yifei Yang on 11/8/21.
//

#include <normal/catalogue/local-fs/LocalFSTable.h>

using namespace std;

namespace normal::catalogue::local_fs {

LocalFSTable::LocalFSTable(const string &name,
                           const shared_ptr<arrow::Schema> &schema,
                           const shared_ptr<format::Format> &format,
                           const unordered_map<string, int> &apxColumnLengthMap,
                           int apxRowLength,
                           const unordered_set<string> &zonemapColumnNames,
                           const vector<shared_ptr<LocalFSPartition>> &localFsPartitions,
                           const shared_ptr<CatalogueEntry> &catalogueEntry) :
  Table(name, schema, format, apxColumnLengthMap, apxRowLength, zonemapColumnNames, catalogueEntry),
  localFSPartitions_(localFsPartitions) {}

}
