//
// Created by matt on 1/8/20.
//

#include <normal/tuple/TupleSetIndex.h>
#include <normal/tuple/ColumnName.h>
#include <normal/tuple/Globals.h>
#include <fmt/format.h>
#include <utility>

using namespace normal::tuple;

TupleSetIndex::TupleSetIndex(vector<string> columnNames,
                             vector<int> columnIndexes,
                             shared_ptr<::arrow::Table> table,
                             unordered_multimap<shared_ptr<TupleKey>, int64_t, TupleKeyPointerHash, TupleKeyPointerPredicate> valueRowMap) :
	columnNames_(move(columnNames)),
	columnIndexes_(move(columnIndexes)), 
	table_(move(table)),
	valueRowMap_(move(valueRowMap)) {}

tl::expected<shared_ptr<TupleSetIndex>, string>
TupleSetIndex::make(const vector<string> &columnNames, const shared_ptr<::arrow::Table> &table) {
  // Get the column indexes, checking the column exists
  vector<int> columnIndexes;
  for (const auto &columnName: columnNames) {
    auto columnIndex = table->schema()->GetFieldIndex(ColumnName::canonicalize(columnName));
    if (columnIndex == -1) {
      return tl::make_unexpected(fmt::format("Cannot make TupleSetIndex. Column '{}' does not exist", columnName));
    }
    columnIndexes.emplace_back(columnIndex);
  }

  // Build valueRowMap
  auto expValueRowMap = build(columnIndexes, 0, table);
  if (!expValueRowMap.has_value()) {
    return tl::make_unexpected(expValueRowMap.error());
  }
  auto valueRowMap = expValueRowMap.value();

  return make_shared<TupleSetIndex>(columnNames, columnIndexes, table, valueRowMap);
}

tl::expected<unordered_multimap<shared_ptr<TupleKey>, int64_t, TupleKeyPointerHash, TupleKeyPointerPredicate>, string>
TupleSetIndex::build(const vector<int>& columnIndexes,
                     int64_t rowIndexOffset, 
                     const shared_ptr<::arrow::Table> &table) {
  unordered_multimap<shared_ptr<TupleKey>, int64_t, TupleKeyPointerHash, TupleKeyPointerPredicate> valueIndexMap{};

  // Read the table a batch at a time
  ::arrow::Result<shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;
  ::arrow::TableBatchReader reader{*table};
  reader.set_chunksize((int64_t) DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
    return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

  int64_t tableRow = rowIndexOffset;
  while (recordBatch) {
    // Insert
    for (int64_t batchRow = 0; batchRow < recordBatch->num_rows(); ++batchRow) {
      // Make tupleKey
      const auto &expTupleKey = TupleKeyBuilder::make(batchRow, columnIndexes, *recordBatch);
      if (!expTupleKey.has_value()) {
        return tl::make_unexpected(expTupleKey.error());
      }
      const auto &tupleKey = expTupleKey.value();
      
      valueIndexMap.emplace(tupleKey, tableRow);
      ++tableRow;
    }

    // Read a batch
    recordBatchResult = reader.Next();
    if (!recordBatchResult.ok()) {
      return tl::make_unexpected(recordBatchResult.status().message());
    }
    recordBatch = *recordBatchResult;
  }

  assert(valueIndexMap.size() == static_cast<size_t>(table->num_rows()));
  return valueIndexMap;
}

tl::expected<void, string> TupleSetIndex::put(const shared_ptr<::arrow::Table> &table) {
  // Need to synchronize schemas (schemas can be the same but in different orders)
  shared_ptr<::arrow::Table> alignedTable;
  auto schema = table_->schema();
  if (!schema->Equals(table->schema())) {
    vector<shared_ptr<::arrow::ChunkedArray>> newChunkedArrays;
    for (auto const &field: schema->fields()) {
      auto chunkedArray = table->GetColumnByName(field->name());
      if (!chunkedArray) {
        return tl::make_unexpected("Schemas not compatible");
      }
      newChunkedArrays.emplace_back(chunkedArray);
    }
    alignedTable = ::arrow::Table::Make(schema, newChunkedArrays);
  } else {
    alignedTable = table;
  }

  // Make columnIndexes from the incoming table
  vector<int> columnIndexes;
  for (const auto &columnName: columnNames_) {
    auto columnIndex = alignedTable->schema()->GetFieldIndex(columnName);
    if (columnIndex == -1) {
      return tl::make_unexpected(fmt::format("Cannot add table to TupleSetIndex. Column '{}' does not exist", columnName));
    }
    columnIndexes.emplace_back(columnIndex);
  }
  if (columnIndexes_ != columnIndexes) {
    return tl::make_unexpected(fmt::format("Cannot add table to TupleSetIndex. Incoming column positions are different than exising ones"));
  }

  // Build valueRowIndexMap
  const auto &expValueRowIndexMap = build(columnIndexes, table_->num_rows(), alignedTable);
  if (!expValueRowIndexMap.has_value())
    return tl::make_unexpected(expValueRowIndexMap.error());
  const auto &valueRowIndexMap = expValueRowIndexMap.value();

  // Insert them into existing map
  valueRowMap_.insert(valueRowIndexMap.begin(), valueRowIndexMap.end());

  // Save the incoming table
  auto appendResult = ::arrow::ConcatenateTables({table_, alignedTable});
  if (!appendResult.ok())
    return tl::make_unexpected(appendResult.status().message());
  table_ = *appendResult;

  assert(valueRowMap_.size() == static_cast<size_t>(table_->num_rows()));
  return {};
}

tl::expected<void, string> TupleSetIndex::merge(const shared_ptr<TupleSetIndex> &other) {
  int64_t rowOffset = table_->num_rows();

  // Add the other rows to hashtable, offsetting their row numbers
  for (const auto &valueIndexMapIterator: other->valueRowMap_) {
    valueRowMap_.emplace(valueIndexMapIterator.first, valueIndexMapIterator.second + rowOffset);
  }

  // Add the other's table to the existing table
  auto appendResult = ::arrow::ConcatenateTables({table_, other->table_});
  if (!appendResult.ok()) {
    return tl::make_unexpected(appendResult.status().message());
  }
  table_ = *appendResult;

  assert(valueRowMap_.size() == static_cast<size_t>(table_->num_rows()));
  return {};
}

tl::expected<void, string> TupleSetIndex::validate() {
  unordered_multimap<shared_ptr<TupleKey>, int64_t, TupleKeyPointerHash, TupleKeyPointerPredicate> valueIndexMap{};
  
  // Read the table a batch at a time
  ::arrow::Result<shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;
  ::arrow::TableBatchReader reader{*table_};
  reader.set_chunksize((int64_t) DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
    return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

  int64_t tableRow = 0;
  while (recordBatch) {
    // Insert
    for (int64_t batchRow = 0; batchRow < recordBatch->num_rows(); ++batchRow) {
      // Make tupleKey
      const auto &expTupleKey = TupleKeyBuilder::make(batchRow, columnIndexes_, *recordBatch);
      if (!expTupleKey.has_value()) {
        return tl::make_unexpected(expTupleKey.error());
      }
      const auto &tupleKey = expTupleKey.value();

      auto indexes = find(tupleKey);
      bool found = false;
      for(long long index: indexes){
        SPDLOG_DEBUG("  Index for tupleKey '{}' points to row {}", tupleKey->toString(), index);
        if(index == tableRow) {
          SPDLOG_DEBUG("  FOUND!");
          found = true;
          break;
        }
      }
      if(!found) {
        SPDLOG_DEBUG("  NOT FOUND!");
        return tl::make_unexpected(fmt::format("Index invalid. Value found at row {} is '{}' was not found in the index",
                                               tableRow, tupleKey->toString()));
      }

      ++tableRow;
    }

    // Read a batch
    recordBatchResult = reader.Next();
    if (!recordBatchResult.ok()) {
      return tl::make_unexpected(recordBatchResult.status().message());
    }
    recordBatch = *recordBatchResult;
  }

  return {};
}

vector<int64_t> TupleSetIndex::find(const shared_ptr<TupleKey> &tupleKey) {
  vector<int64_t> rowIndexes;
  auto range = valueRowMap_.equal_range(tupleKey);
  for (auto it = range.first; it != range.second; ++it) {
    rowIndexes.emplace_back(it->second);
  }
  return rowIndexes;
}

const shared_ptr<::arrow::Table> &TupleSetIndex::getTable() const {
  return table_;
}

int64_t TupleSetIndex::size() const {
  return table_->num_rows();
}

tl::expected<void, string> TupleSetIndex::combine() {
  auto expectedTable = table_->CombineChunks(::arrow::default_memory_pool());
  if(expectedTable.ok())
    table_ = *expectedTable;
  else
    return tl::make_unexpected(expectedTable.status().message());
  return {};
}

string TupleSetIndex::toString() const {
  stringstream ss;
  for (const auto &valueRowIndex: valueRowMap_) {
    ss << fmt::format("[{}] : {}\n", valueRowIndex.first->toString(), valueRowIndex.second);
  }
  return ss.str();
}
