//
// Created by Yifei Yang on 11/8/21.
//

#include <normal/catalogue/s3/S3Table.h>

using namespace std;

namespace normal::catalogue::s3 {

S3Table::S3Table(const string &name,
                 const shared_ptr<arrow::Schema> &schema,
                 const shared_ptr<format::Format> &format,
                 const unordered_map<string, int> &apxColumnLengthMap,
                 int apxRowLength,
                 const unordered_set<string> &zonemapColumnNames,
                 const vector<shared_ptr<S3Partition>> &s3Partitions,
                 const shared_ptr<CatalogueEntry> &catalogueEntry) :
  Table(name, schema, format, apxColumnLengthMap, apxRowLength, zonemapColumnNames, catalogueEntry),
  s3Partitions_(s3Partitions) {}

const vector<shared_ptr<S3Partition>> &S3Table::getS3Partitions() const {
  return s3Partitions_;
}

}
