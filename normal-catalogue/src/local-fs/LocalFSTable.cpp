//
// Created by Yifei Yang on 11/8/21.
//

#include <normal/catalogue/local-fs/LocalFSTable.h>

using namespace std;

namespace normal::catalogue::local_fs {

LocalFSTable::LocalFSTable(const string &name,
                           const shared_ptr<arrow::Schema> &schema,
                           const unordered_map<string, int> &columnLengthMap,
                           int rowLength,
                           const unordered_set<string> &zoneMapColumnNames,
                           const vector<shared_ptr<LocalFSPartition>> &localFsPartitions,
                           const shared_ptr<CatalogueEntry> &catalogueEntry) :
  Table(name, schema, columnLengthMap, rowLength, zoneMapColumnNames, catalogueEntry),
  localFSPartitions_(localFsPartitions) {}

}
