//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3RELATION_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3RELATION_H

#include <normal/catalogue/Relation.h>
#include <normal/catalogue/local-fs/LocalFSPartition.h>
#include <vector>

using namespace std;

namespace normal::catalogue::local_fs {

class LocalFSRelation: public Relation {
public:
  LocalFSRelation(const string &name,
             const shared_ptr<CatalogueEntry> &catalogueEntry,
             const vector<shared_ptr<LocalFSPartition>> &localFSPartitions);

private:
  vector<shared_ptr<LocalFSPartition>> localFSPartitions_;
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3RELATION_H
