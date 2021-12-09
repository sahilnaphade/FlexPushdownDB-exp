//
// Created by matt on 3/8/20.
//

#include <normal/executor/physical/hashjoin/RecordBatchHashJoiner.h>
#include <normal/tuple/TupleSetIndexFinder.h>
#include <normal/tuple/ArrayAppenderWrapper.h>
#include <normal/tuple/TupleKey.h>

using namespace normal::executor::physical::hashjoin;

RecordBatchHashJoiner::RecordBatchHashJoiner(std::shared_ptr<TupleSetIndex> buildTupleSetIndex,
                   std::vector<std::string> probeJoinColumnNames,
									 std::shared_ptr<::arrow::Schema> outputSchema,
                   std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice) :
	buildTupleSetIndex_(std::move(buildTupleSetIndex)),
	probeJoinColumnNames_(std::move(probeJoinColumnNames)),
	outputSchema_(std::move(outputSchema)),
	neededColumnIndice_(std::move(neededColumnIndice)),
	joinedArrayVectors_{static_cast<size_t>(outputSchema_->num_fields())} {
}

tl::expected<std::shared_ptr<RecordBatchHashJoiner>, std::string>
RecordBatchHashJoiner::make(const std::shared_ptr<TupleSetIndex> &buildTupleSetIndex,
						const std::vector<std::string> &probeJoinColumnNames,
						const std::shared_ptr<::arrow::Schema> &outputSchema,
            const std::vector<std::shared_ptr<std::pair<bool, int>>> &neededColumnIndice) {
  const auto &canonicalColumnNames = ColumnName::canonicalize(probeJoinColumnNames);
  return std::make_shared<RecordBatchHashJoiner>(buildTupleSetIndex, canonicalColumnNames, outputSchema, neededColumnIndice);
}

tl::expected<void, std::string>
RecordBatchHashJoiner::join(const std::shared_ptr<::arrow::RecordBatch> &recordBatch) {

  arrow::Status status;

  // Combine the chunks in the build table so we have single arrays for each column
  auto combineResult = buildTupleSetIndex_->combine();
  if (!combineResult)
    return tl::make_unexpected(combineResult.error());

  //  buildTupleSetIndex_->validate();

  auto buildTable = buildTupleSetIndex_->getTable();

  // Create a tupleSetIndexFinder
  const auto &expectedIndexFinder = TupleSetIndexFinder::make(buildTupleSetIndex_, probeJoinColumnNames_, recordBatch);
  if (!expectedIndexFinder.has_value())
	  return tl::make_unexpected(expectedIndexFinder.error());
  auto indexFinder = expectedIndexFinder.value();

  // Create references to each array in the index
  ::arrow::ArrayVector buildColumns;
  for (const auto &column: buildTable->columns()) {
    buildColumns.emplace_back(column->chunk(0));
  }

  // Create references to each array in the record batch
  std::vector<std::shared_ptr<::arrow::Array>> probeColumns{static_cast<size_t>(recordBatch->num_columns())};
  for (int c = 0; c < recordBatch->num_columns(); ++c) {
    probeColumns[c] = recordBatch->column(c);
  }

  // create appenders to create the destination arrays
  std::vector<std::shared_ptr<ArrayAppender>> appenders{static_cast<size_t>(outputSchema_->num_fields())};

  for (int c = 0; c < outputSchema_->num_fields(); ++c) {
    auto expectedAppender = ArrayAppenderBuilder::make(outputSchema_->field(c)->type(), 0);
    if (!expectedAppender.has_value())
      return tl::make_unexpected(expectedAppender.error());
    appenders[c] = expectedAppender.value();
  }

  // Iterate through the probe join column
  for (int64_t pr = 0; pr < recordBatch->num_rows(); ++pr) {

    // Find matched rows in the build column
    const auto expBuildRows = indexFinder->find(pr);
    if (!expBuildRows.has_value()) {
      return tl::make_unexpected(expBuildRows.error());
    }
    auto buildRows = expBuildRows.value();

    // Iterate the matched rows in the build column
    for (const auto br: buildRows) {

      // Iterate needed columns
      for (size_t c = 0; c < neededColumnIndice_.size(); ++c) {

        // build column
        if (neededColumnIndice_[c]->first) {
          auto appendResult = appenders[c]->safeAppendValue(buildColumns[neededColumnIndice_[c]->second], br);
          if(!appendResult) return appendResult;
        }

        // probe column
        else {
          auto appendResult = appenders[c]->safeAppendValue(probeColumns[neededColumnIndice_[c]->second], pr);
          if(!appendResult) return appendResult;
        }
      }
    }
  }

  // Create arrays from the appenders
  for (size_t c = 0; c < appenders.size(); ++c) {
    auto expectedArray = appenders[c]->finalize();
    if (!expectedArray.has_value())
      return tl::make_unexpected(expectedArray.error());
    if (expectedArray.value()->length() > 0)
      joinedArrayVectors_[c].emplace_back(expectedArray.value());
  }

  return {};
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
RecordBatchHashJoiner::toTupleSet() {
  arrow::Status status;

  // Make chunked arrays
  std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunkedArrays;
  for (const auto &joinedArrayVector: joinedArrayVectors_) {
    // check empty
    if (joinedArrayVector.empty()) {
      return TupleSet::make(outputSchema_);
    }

    auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(joinedArrayVector);
    chunkedArrays.emplace_back(chunkedArray);
  }

  auto joinedTable = ::arrow::Table::Make(outputSchema_, chunkedArrays);
  auto joinedTupleSet = TupleSet::make(joinedTable);

  joinedArrayVectors_.clear();
  return joinedTupleSet;
}
