//
// Created by Yifei Yang on 11/9/21.
//

#include <normal/catalogue/s3/S3CatalogueEntryReader.h>
#include <normal/tuple/ColumnName.h>
#include <normal/util/Util.h>
#include <nlohmann/json.hpp>
#include <fmt/format.h>

using namespace normal::tuple;
using namespace normal::util;

namespace normal::catalogue::s3 {

shared_ptr<S3CatalogueEntry>
S3CatalogueEntryReader::readS3CatalogueEntry(const shared_ptr<Catalogue> &catalogue,
                                             const string &s3Bucket,
                                             const string &schemaName) {
  // create an S3CatalogueEntry
  shared_ptr<S3CatalogueEntry> s3CatalogueEntry = make_shared<S3CatalogueEntry>(schemaName, s3Bucket, catalogue);

  // read metadata files
  filesystem::path metadataPath = catalogue->getMetadataPath().append(schemaName);
  auto schemaJObj = json::parse(readFile(metadataPath.append("schema.json")));
  metadataPath = metadataPath.parent_path();
  auto statsJObj = json::parse(readFile(metadataPath.append("stats.json")));
  metadataPath = metadataPath.parent_path();
  auto zonemapJObj = json::parse(readFile(metadataPath.append("zonemap.json")));

  // get all table names
  unordered_set<string> tableNames;
  for (const auto &tableSchemaJObj: schemaJObj["tables"].get<vector<json>>()) {
    tableNames.emplace(tableSchemaJObj["name"].get<string>());
  }

  // member variables to make an S3Table
  unordered_map<string, shared_ptr<arrow::Schema>> schemaMap;
  unordered_map<string, unordered_map<string, int>> apxColumnLengthMapMap;
  unordered_map<string, int> apxRowLengthMap;
  unordered_map<string, unordered_set<string>> zonemapColumnNamesMap;
  unordered_map<string, vector<shared_ptr<S3Partition>>> s3PartitionsMap;

  // read schema
  readSchema(schemaJObj, s3Bucket, schemaName, schemaMap, s3PartitionsMap);

  // read stats
  readStats(statsJObj, apxColumnLengthMapMap, apxRowLengthMap);

  // read zonemap
  readZonemap(zonemapJObj, schemaMap, s3PartitionsMap, zonemapColumnNamesMap);

  // read partition size from s3 listObject
  readPartitionSize(s3PartitionsMap);

  // make S3Tables
  for (const auto &tableName: tableNames) {
    shared_ptr<S3Table> s3Table = make_shared<S3Table>(tableName,
                                                       schemaMap.find(tableName)->second,
                                                       apxColumnLengthMapMap.find(tableName)->second,
                                                       apxRowLengthMap.find(tableName)->second,
                                                       zonemapColumnNamesMap.find(tableName)->second,
                                                       s3PartitionsMap.find(tableName)->second,
                                                       s3CatalogueEntry);
    s3CatalogueEntry->addS3Table(s3Table);
  }

  return s3CatalogueEntry;
}

void S3CatalogueEntryReader::readSchema(const json &schemaJObj,
                                        const string &s3Bucket,
                                        const string &schemaName,
                                        unordered_map<string, shared_ptr<arrow::Schema>> &schemaMap,
                                        unordered_map<string, vector<shared_ptr<S3Partition>>> &s3PartitionsMap) {
  for (const auto &tableSchemasJObj: schemaJObj["tables"].get<vector<json>>()) {
    const string &tableName = tableSchemasJObj["name"].get<string>();
    // fields
    vector<shared_ptr<arrow::Field>> fields;
    for (const auto &fieldJObj: tableSchemasJObj["fields"].get<vector<json>>()) {
      const string &fieldName = ColumnName::canonicalize(fieldJObj["name"].get<string>());
      const string &fieldTypeStr = fieldJObj["type"].get<string>();
      const auto &fieldType = strToDataType(fieldTypeStr);
      auto field = make_shared<arrow::Field>(fieldName, fieldType);
      fields.emplace_back(field);
    }
    auto schema = make_shared<arrow::Schema>(fields);
    schemaMap.emplace(tableName, schema);

    // partitions
    vector<shared_ptr<S3Partition>> s3Partitions;
    int numPartitions = tableSchemasJObj["numPartitions"].get<int>();
    if (numPartitions == 1) {
      string s3Object = schemaName + tableName + ".tbl";
      s3Partitions.emplace_back(make_shared<S3Partition>(s3Bucket, s3Object));
    } else {
      string s3ObjectDir = schemaName + tableName + "_sharded/";
      for (int i = 0; i < numPartitions; ++i) {
        string s3Object = s3ObjectDir + tableName + ".tbl." + to_string(i);
        s3Partitions.emplace_back(make_shared<S3Partition>(s3Bucket, s3Object));
      }
    }
    s3PartitionsMap.emplace(tableName, s3Partitions);
  }
}

void S3CatalogueEntryReader::readStats(const json &statsJObj,
                                       unordered_map<string, unordered_map<string, int>> &apxColumnLengthMapMap,
                                       unordered_map<string, int> &apxRowLengthMap) {
  for (const auto &tableStatsJObj: statsJObj["tables"].get<vector<json>>()) {
    const string &tableName = tableStatsJObj["name"].get<string>();
    unordered_map<string, int> apxColumnLengthMap;
    int apxRowLength = 0;
    for (const auto &apxColumnLengthIt: tableStatsJObj["stats"]["apxColumnLength"].get<unordered_map<string, int>>()) {
      string columnName = ColumnName::canonicalize(apxColumnLengthIt.first);
      int apxColumnLength = apxColumnLengthIt.second;
      apxColumnLengthMap.emplace(columnName, apxColumnLength);
      apxRowLength += apxColumnLength;
    }
    apxColumnLengthMapMap.emplace(tableName, apxColumnLengthMap);
    apxRowLengthMap.emplace(tableName, apxRowLength);
  }
}

void S3CatalogueEntryReader::readZonemap(const json &zonemapJObj,
                                         const unordered_map<string, shared_ptr<arrow::Schema>> &schemaMap,
                                         unordered_map<string, vector<shared_ptr<S3Partition>>> &s3PartitionsMap,
                                         unordered_map<string, unordered_set<string>> &zonemapColumnNamesMap) {
  for (const auto &tableZonemapsJObj: zonemapJObj["tables"].get<vector<json>>()) {
    const string &tableName = tableZonemapsJObj["name"].get<string>();
    const shared_ptr<arrow::Schema> &schema = schemaMap.find(tableName)->second;
    const vector<shared_ptr<S3Partition>> &s3Partitions = s3PartitionsMap.find(tableName)->second;

    unordered_set<string> zonemapColumnNames;
    for (const auto &tableZonemapJObj: tableZonemapsJObj["zonemap"].get<vector<json>>()) {
      const string &fieldName = ColumnName::canonicalize(tableZonemapJObj["field"].get<string>());
      zonemapColumnNames.emplace(fieldName);
      const shared_ptr<arrow::DataType> fieldType = schema->GetFieldByName(fieldName)->type();
      const auto valuePairsJArr = tableZonemapJObj["valuePairs"].get<vector<json>>();

      for (size_t i = 0; i < s3Partitions.size(); ++i) {
        const auto &s3Partition = s3Partitions[i];
        const auto valuePairJObj = valuePairsJArr[i];
        s3Partition->addMinMax(fieldName, jsonToMinMaxLiterals(valuePairJObj, fieldType));
      }
    }
    zonemapColumnNamesMap.emplace(tableName, zonemapColumnNames);
  }
}

void
S3CatalogueEntryReader::readPartitionSize(unordered_map<string, vector<shared_ptr<S3Partition>>> &s3PartitionsMap) {

}

shared_ptr<arrow::DataType> S3CatalogueEntryReader::strToDataType(const string &str) {
  if (str == "int32" || str == "int") {
    return arrow::int32();
  } else if (str == "int64" || str == "long") {
    return arrow::int64();
  } else if (str == "float64" || str == "double") {
    return arrow::float64();
  } else if (str == "utf8" || str == "string") {
    return arrow::utf8();
  } else if (str == "boolean" || str == "bool") {
    return arrow::boolean();
  } else {
    throw runtime_error(fmt::format("Unsupported data type: {}", str));
  }
}

pair<shared_ptr<Expression>, shared_ptr<Expression>>
S3CatalogueEntryReader::jsonToMinMaxLiterals(const json &jObj, const shared_ptr<arrow::DataType> &datatype) {
  if (datatype->id() == arrow::int32()->id()) {
    return make_pair(num_lit<arrow::Int32Type>(jObj["min"].get<int>()),
                     num_lit<arrow::Int32Type>(jObj["max"].get<int>()));
  } else if (datatype->id() == arrow::int64()->id()) {
    return make_pair(num_lit<arrow::Int64Type>(jObj["min"].get<long>()),
                     num_lit<arrow::Int64Type>(jObj["max"].get<long>()));
  } else if (datatype->id() == arrow::float64()->id()) {
    return make_pair(num_lit<arrow::DoubleType>(jObj["min"].get<double>()),
                     num_lit<arrow::DoubleType>(jObj["max"].get<double>()));
  } else if (datatype->id() == arrow::uint8()->id()) {
    return make_pair(str_lit(jObj["min"].get<string>()),
                     str_lit(jObj["max"].get<string>()));
  } else {
    throw runtime_error(fmt::format("Unsupported data type: {}", datatype->name()));
  }
}

}
