//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3RELATION_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3RELATION_H

#include <normal/catalogue/Relation.h>
#include <normal/catalogue/s3/S3Partition.h>
#include <vector>

using namespace std;

namespace normal::catalogue::s3 {

class S3Relation: public Relation {
public:
  S3Relation(const string &name,
             const shared_ptr<CatalogueEntry> &catalogueEntry,
             const vector<shared_ptr<S3Partition>> &s3Partitions);

private:
  vector<shared_ptr<S3Partition>> s3Partitions_;
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3RELATION_H
