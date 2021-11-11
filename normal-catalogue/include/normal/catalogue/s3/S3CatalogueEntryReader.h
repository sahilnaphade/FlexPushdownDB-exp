//
// Created by Yifei Yang on 11/9/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3CATALOGUEENTRYREADER_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3CATALOGUEENTRYREADER_H

#include <normal/catalogue/s3/S3CatalogueEntry.h>
#include <normal/catalogue/Catalogue.h>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <normal/expression/gandiva/StringLiteral.h>
#include <nlohmann/json.hpp>
#include <aws/s3/S3Client.h>

using namespace normal::expression::gandiva;
using namespace Aws::S3;
using namespace std;
using json = nlohmann::json;

namespace normal::catalogue::s3 {

class S3CatalogueEntryReader {
public:
  static shared_ptr<S3CatalogueEntry> readS3CatalogueEntry(const shared_ptr<Catalogue> &catalogue,
                                                           const string &s3Bucket,
                                                           const string &schemaName,
                                                           const shared_ptr<S3Client> &s3Client);

private:
  static void readSchema(const json &schemaJObj,
                         const string &s3Bucket,
                         const string &schemaName,
                         unordered_map<string, shared_ptr<arrow::Schema>> &schemaMap,
                         unordered_map<string, vector<shared_ptr<S3Partition>>> &s3PartitionsMap);
  static void readStats(const json &statsJObj,
                        unordered_map<string, unordered_map<string, int>> &apxColumnLengthMapMap,
                        unordered_map<string, int> &apxRowLengthMap);
  static void readZonemap(const json &zonemapJObj,
                          const unordered_map<string, shared_ptr<arrow::Schema>> &schemaMap,
                          unordered_map<string, vector<shared_ptr<S3Partition>>> &s3PartitionsMap,
                          unordered_map<string, unordered_set<string>> &zonemapColumnNamesMap);
  static void readPartitionSize(const shared_ptr<S3Client> &s3Client,
                                const string &s3Bucket,
                                const string &schemaName,
                                unordered_map<string, vector<shared_ptr<S3Partition>>> &s3PartitionsMap);

  static shared_ptr<arrow::DataType> strToDataType(const string &str);
  static pair<shared_ptr<Expression>, shared_ptr<Expression>>
    jsonToMinMaxLiterals(const json &jObj, const shared_ptr<arrow::DataType> &datatype);
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3CATALOGUEENTRYREADER_H
