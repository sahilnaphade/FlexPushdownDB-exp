//
// Created by Yifei Yang on 11/8/21.
//

#include <normal/catalogue/local-fs/LocalFSRelation.h>

using namespace std;

namespace normal::catalogue::local_fs {

LocalFSRelation::LocalFSRelation(const string &name,
                       const shared_ptr<CatalogueEntry> &catalogueEntry,
                       const vector<shared_ptr<LocalFSPartition>> &localFSPartitions) :
  Relation(name, catalogueEntry),
  localFSPartitions_(localFSPartitions) {}

}
