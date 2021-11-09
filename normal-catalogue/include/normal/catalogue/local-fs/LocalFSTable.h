//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSTABLE_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSTABLE_H

#include <normal/catalogue/Table.h>
#include <normal/catalogue/local-fs/LocalFSPartition.h>
#include <vector>

using namespace std;

namespace normal::catalogue::local_fs {

class LocalFSTable: public Table {
public:
  LocalFSTable(const string &name,
               const shared_ptr<arrow::Schema> &schema,
               const unordered_map<string, int> &columnLengthMap,
               int rowLength,
               const unordered_set<string> &zoneMapColumnNames,
               const vector<shared_ptr<LocalFSPartition>> &localFsPartitions,
               const shared_ptr<CatalogueEntry> &catalogueEntry);

private:
  vector<shared_ptr<LocalFSPartition>> localFSPartitions_;
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSTABLE_H
