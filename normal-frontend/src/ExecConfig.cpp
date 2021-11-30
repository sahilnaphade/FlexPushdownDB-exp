//
// Created by Yifei Yang on 10/12/21.
//

#include <normal/frontend/ExecConfig.h>
#include <normal/cache/policy/LRUCachingPolicy.h>
#include <normal/cache/policy/LFUCachingPolicy.h>
#include <normal/cache/policy/LFUSCachingPolicy.h>
#include <normal/cache/policy/WLFUCachingPolicy.h>
#include <normal/catalogue/CatalogueEntry.h>
#include <normal/catalogue/s3/S3CatalogueEntryReader.h>
#include <normal/util/Util.h>
#include <fmt/format.h>
#include <string>
#include <utility>

using namespace normal::catalogue::s3;
using namespace normal::util;

namespace normal::frontend {

ExecConfig::ExecConfig(const shared_ptr<Mode> &mode,
                       const shared_ptr<CachingPolicy> &cachingPolicy,
                       string s3Bucket,
                       string schemaName,
                       int parallelDegree,
                       bool showOpTimes,
                       bool showScanMetrics) :
  mode_(mode),
  cachingPolicy_(cachingPolicy),
  s3Bucket_(move(s3Bucket)),
  schemaName_(move(schemaName)),
  parallelDegree_(parallelDegree),
  showOpTimes_(showOpTimes),
  showScanMetrics_(showScanMetrics) {}

shared_ptr<ExecConfig> ExecConfig::parseExecConfig(const shared_ptr<Catalogue> &catalogue,
                                                   const shared_ptr<AWSClient> &awsClient) {
  // read config
  unordered_map<string, string> configMap = readConfig("exec.conf");
  string s3Bucket = configMap["S3_BUCKET"];
  string schemaName = configMap["SCHEMA_NAME"];
  size_t cacheSize = parseCacheSize(configMap["CACHE_SIZE"]);
  shared_ptr<Mode> mode = parseMode(configMap["MODE"]);
  int parallelDegree = stoi(configMap["PARALLEL_DEGREE"]);
  bool showOpTimes = parseBool(configMap["SHOW_OP_TIMES"]);
  bool showScanMetrics = parseBool(configMap["SHOW_SCAN_METRICS"]);

  // catalogue entry
  shared_ptr<CatalogueEntry> catalogueEntry;
  const auto expCatalogueEntry = catalogue->getEntry(fmt::format("s3://{}/{}", s3Bucket, schemaName));
  if (expCatalogueEntry.has_value()) {
    catalogueEntry = expCatalogueEntry.value();
  } else {
    catalogueEntry = S3CatalogueEntryReader::readS3CatalogueEntry(catalogue, s3Bucket, schemaName, awsClient->getS3Client());
    catalogue->putEntry(catalogueEntry);
  }

  // caching policy
  shared_ptr<CachingPolicy> cachingPolicy = parseCachingPolicy(configMap["CACHING_POLICY"], cacheSize, catalogueEntry);

  return make_shared<ExecConfig>(mode, cachingPolicy, s3Bucket, schemaName, parallelDegree, showOpTimes, showScanMetrics);
}

size_t ExecConfig::parseCacheSize(const string& stringToParse) {
  size_t cacheSize;
  if (stringToParse.substr(stringToParse.length() - 2) == "GB"
      || stringToParse.substr(stringToParse.length() - 2) == "MB"
      || stringToParse.substr(stringToParse.length() - 2) == "KB") {
    auto cacheSizeStr = stringToParse.substr(0, stringToParse.length() - 2);
    stringstream ss(cacheSizeStr);
    ss >> cacheSize;
    if (stringToParse.substr(stringToParse.length() - 2) == "GB") {
      return cacheSize * 1024 * 1024 * 1024;
    } else if (stringToParse.substr(stringToParse.length() - 2) == "MB") {
      return cacheSize * 1024 * 1024;
    } else {
      return cacheSize * 1024;
    }
  } else if (stringToParse.substr(stringToParse.length() - 1) == "B") {
    auto cacheSizeStr = stringToParse.substr(0, stringToParse.length() - 1);
    stringstream ss(cacheSizeStr);
    ss >> cacheSize;
    return cacheSize;
  }
  return 0;
}

shared_ptr<Mode> ExecConfig::parseMode(const string& stringToParse) {
  if (stringToParse == "PULLUP") {
    return Mode::pullupMode();
  } else if (stringToParse == "PUSHDOWN_ONLY") {
    return Mode::pushdownOnlyMode();
  } else if (stringToParse == "CACHING_ONLY") {
    return Mode::cachingOnlyMode();
  } else if (stringToParse == "HYBRID") {
    return Mode::hybridMode();
  }
  throw runtime_error(fmt::format("Unknown mode: {}", stringToParse));
}

shared_ptr<CachingPolicy> ExecConfig::parseCachingPolicy(const string& stringToParse,
                                                         size_t cacheSize,
                                                         const shared_ptr<CatalogueEntry> &catalogueEntry) {
  if (stringToParse == "LRU") {
    return make_shared<LRUCachingPolicy>(cacheSize, catalogueEntry);
  } else if (stringToParse == "LFU") {
    return make_shared<LFUCachingPolicy>(cacheSize, catalogueEntry);
  } else if (stringToParse == "LFU-S") {
    return make_shared<LFUSCachingPolicy>(cacheSize, catalogueEntry);
  } else if (stringToParse == "W-LFU") {
    return make_shared<WLFUCachingPolicy>(cacheSize, catalogueEntry);
  }
  throw runtime_error(fmt::format("Unknown caching policy: {}", stringToParse));
}

const shared_ptr<Mode> &ExecConfig::getMode() const {
  return mode_;
}

const shared_ptr<CachingPolicy> &ExecConfig::getCachingPolicy() const {
  return cachingPolicy_;
}

const string &ExecConfig::getS3Bucket() const {
  return s3Bucket_;
}

const string &ExecConfig::getSchemaName() const {
  return schemaName_;
}

int ExecConfig::getParallelDegree() const {
  return parallelDegree_;
}

bool ExecConfig::showOpTimes() const {
  return showOpTimes_;
}

bool ExecConfig::showScanMetrics() const {
  return showScanMetrics_;
}
}
