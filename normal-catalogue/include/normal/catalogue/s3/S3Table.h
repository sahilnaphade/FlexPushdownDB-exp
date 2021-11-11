//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3TABLE_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3TABLE_H

#include <normal/catalogue/Table.h>
#include <normal/catalogue/s3/S3Partition.h>
#include <vector>

using namespace std;

namespace normal::catalogue::s3 {

class S3Table: public Table {
public:
  S3Table(const string &name,
          const shared_ptr<arrow::Schema> &schema,
          const shared_ptr<format::Format> &format,
          const unordered_map<string, int> &apxColumnLengthMap,
          int apxRowLength,
          const unordered_set<string> &zonemapColumnNames,
          const vector<shared_ptr<S3Partition>> &s3Partitions,
          const shared_ptr<CatalogueEntry> &catalogueEntry);

private:
  vector<shared_ptr<S3Partition>> s3Partitions_;
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3TABLE_H
