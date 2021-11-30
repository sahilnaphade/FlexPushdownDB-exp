//
// Created by Yifei Yang on 10/12/21.
//

#ifndef NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_EXECCONFIG_H
#define NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_EXECCONFIG_H

#include <normal/cache/policy/CachingPolicy.h>
#include <normal/plan/Mode.h>
#include <normal/catalogue/Catalogue.h>
#include <normal/aws/AWSClient.h>
#include <unordered_map>
#include <string>

using namespace normal::cache::policy;
using namespace normal::plan;
using namespace normal::catalogue;
using namespace normal::aws;

namespace normal::frontend{

class ExecConfig {
public:
  ExecConfig(const shared_ptr<Mode> &mode,
             const shared_ptr<CachingPolicy> &cachingPolicy,
             string s3Bucket,
             string schemaName,
             int parallelDegree,
             bool showOpTimes,
             bool showScanMetrics);

  static shared_ptr<ExecConfig> parseExecConfig(const shared_ptr<Catalogue> &catalogue,
                                                const shared_ptr<AWSClient> &awsClient);

  const shared_ptr<Mode> &getMode() const;
  const shared_ptr<CachingPolicy> &getCachingPolicy() const;
  const string &getS3Bucket() const;
  const string &getSchemaName() const;
  int getParallelDegree() const;
  bool showOpTimes() const;
  bool showScanMetrics() const;

private:
  static size_t parseCacheSize(const string& stringToParse);
  static shared_ptr<Mode> parseMode(const string& stringToParse);
  static shared_ptr<CachingPolicy> parseCachingPolicy(const string& stringToParse,
                                                      size_t cacheSize,
                                                      const shared_ptr<CatalogueEntry> &catalogueEntry);

  shared_ptr<Mode> mode_;
  shared_ptr<CachingPolicy> cachingPolicy_;
  string s3Bucket_;
  string schemaName_;
  int parallelDegree_;
  bool showOpTimes_;
  bool showScanMetrics_;

};

}


#endif //NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_EXECCONFIG_H
