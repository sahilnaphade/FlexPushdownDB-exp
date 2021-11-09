//
// Created by Yifei Yang on 11/8/21.
//

#include <normal/catalogue/s3/S3Relation.h>

using namespace std;

namespace normal::catalogue::s3 {

S3Relation::S3Relation(const string &name,
                       const shared_ptr<CatalogueEntry> &catalogueEntry,
                       const vector<shared_ptr<S3Partition>> &s3Partitions) :
  Relation(name, catalogueEntry),
  s3Partitions_(s3Partitions) {}

}
