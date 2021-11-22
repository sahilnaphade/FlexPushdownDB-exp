//
// Created by Yifei Yang on 11/21/21.
//

#include <normal/executor/physical/transform/PrePToS3PTransformer.h>
#include <normal/executor/physical/prune/PartitionPruner.h>
#include <normal/executor/physical/s3/S3GetPOp.h>
#include <normal/executor/physical/s3/S3SelectPOp.h>
#include <normal/executor/physical/filter/FilterPOp.h>
#include <normal/catalogue/s3/S3Table.h>

using namespace normal::catalogue::s3;

namespace normal::executor::physical {

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToS3PTransformer::transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp) {
  const auto &s3Table = std::static_pointer_cast<S3Table>(filterableScanPrePOp->getTable());
  const auto &partitions = (const vector<shared_ptr<Partition>> &) s3Table->getS3Partitions();
  const auto &partitionPredicates = PartitionPruner::prune(partitions, filterableScanPrePOp->getPredicate());
  vector<string> projectColumnNames{filterableScanPrePOp->getProjectColumnNames().begin(),
                                    filterableScanPrePOp->getProjectColumnNames().end()};

  switch (mode_->id()) {
    case Pullup: return transformFilterableScanPullup(filterableScanPrePOp, partitionPredicates, projectColumnNames);
    case Pushdown: return transformFilterableScanPushdown(filterableScanPrePOp, partitionPredicates, projectColumnNames);
    default: {
      throw runtime_error(fmt::format("Unsupported mode: {}", mode_->toString()));
    }
  }
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToS3PTransformer::transformFilterableScanPullup(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                                    const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates,
                                                    const vector<string> &projectColumnNames) {
  vector<shared_ptr<PhysicalOp>> scanPOps, filterPOps;
  const auto &s3Table = std::static_pointer_cast<S3Table>(filterableScanPrePOp->getTable());

  for (const auto &s3Partition: s3Table->getS3Partitions()) {
    const auto &s3Bucket = s3Partition->getBucket();
    const auto &s3Object = s3Partition->getObject();
    const auto &predicate = partitionPredicates.find(s3Partition)->second;

    const auto &scanPOp = make_shared<s3::S3GetPOp>(fmt::format("S3Get-{}/{}", s3Bucket, s3Object),
                                                   s3Bucket,
                                                   s3Object,
                                                   projectColumnNames,
                                                   0,
                                                   s3Partition->getNumBytes(),
                                                   s3Table,
                                                   awsClient_,
                                                   true,
                                                   false,
                                                   queryId_);
    scanPOps.emplace_back(scanPOp);

    if (predicate) {
      const auto &filterPOp = make_shared<filter::FilterPOp>(fmt::format("Filter-{}/{}", s3Bucket, s3Object),
                                                             predicate,
                                                             s3Table,
                                                             projectColumnNames,
                                                             queryId_);
      filterPOps.emplace_back(filterPOp);
      scanPOp->produce(filterPOp);
      filterPOp->consume(scanPOp);
    }
  }

  if (filterPOps.empty()) {
    return make_pair(scanPOps, scanPOps);
  } else {
    vector<shared_ptr<PhysicalOp>> allPOps;
    allPOps.insert(allPOps.end(), scanPOps.begin(), scanPOps.end());
    allPOps.insert(allPOps.end(), filterPOps.begin(), filterPOps.end());
    return make_pair(filterPOps, allPOps);
  }
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToS3PTransformer::transformFilterableScanPushdown(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                                      const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates,
                                                      const vector<string> &projectColumnNames) {
  vector<shared_ptr<PhysicalOp>> pOps;
  const auto &s3Table = std::static_pointer_cast<S3Table>(filterableScanPrePOp->getTable());

  for (const auto &s3Partition: s3Table->getS3Partitions()) {
    const auto &s3Bucket = s3Partition->getBucket();
    const auto &s3Object = s3Partition->getObject();
    const auto &predicate = partitionPredicates.find(s3Partition)->second;
    const auto &filterSql = genFilterSql(predicate);

    pOps.emplace_back(make_shared<s3::S3SelectPOp>(fmt::format("S3Select-{}/{}", s3Bucket, s3Object),
                                                   s3Bucket,
                                                   s3Object,
                                                   filterSql,
                                                   projectColumnNames,
                                                   0,
                                                   s3Partition->getNumBytes(),
                                                   s3Table,
                                                   awsClient_,
                                                   true,
                                                   false,
                                                   queryId_));
  }

  return make_pair(pOps, pOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToS3PTransformer::transformFilterableScanCachingOnly(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                                         const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates,
                                                         const vector<string> &projectColumnNames) {
  return pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>();
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToS3PTransformer::transformFilterableScanHybrid(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                                    const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates,
                                                    const vector<string> &projectColumnNames) {
  return pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>();
}

string PrePToS3PTransformer::genFilterSql(const shared_ptr<Expression> &predicate) {
  if (predicate != nullptr) {
    std::string filterStr = predicate->alias();
    return " where " + filterStr;
  } else {
    return "";
  }
}

}
