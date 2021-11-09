//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3PARTITION_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3PARTITION_H

#include <normal/catalogue/Partition.h>
#include <string>
#include <memory>

using namespace std;

namespace normal::catalogue::s3 {

class S3Partition: public Partition {
public:
  explicit S3Partition(string bucket, 
                       string object, 
                       long numBytes,
                       const shared_ptr<unordered_map<string, pair<Expression, Expression>>> &zoneMap);

  [[nodiscard]] const string &getBucket() const;
  [[nodiscard]] const string &getObject() const;

  string toString() override;
  size_t hash() override;

  bool equalTo(shared_ptr<Partition> other) override;

  bool operator==(const S3Partition& other);

private:
  string s3Bucket_;
  string s3Object_;
};

}

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3PARTITION_H
