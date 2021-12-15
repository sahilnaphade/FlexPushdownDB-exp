//
// Created by Yifei Yang on 11/21/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_TRANSFORMER_PREPTOS3PTRANSFORMER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_TRANSFORMER_PREPTOS3PTRANSFORMER_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/plan/prephysical/FilterableScanPrePOp.h>
#include <normal/plan/Mode.h>
#include <normal/catalogue/Partition.h>
#include <normal/aws/AWSClient.h>

using namespace normal::plan;
using namespace normal::plan::prephysical;
using namespace normal::expression::gandiva;
using namespace normal::catalogue;
using namespace normal::aws;

namespace normal::executor::physical {

class PrePToS3PTransformer {
public:
  PrePToS3PTransformer(uint prePOpId,
                       const shared_ptr<AWSClient> &awsClient,
                       const shared_ptr<Mode> &mode);

  /**
   * Transform filterable scan prephysical op to physical op
   * @param prePOp: prephysical op
   * @return a pair of connect physical ops (to producer) and current all (cumulative) physical ops
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp);

private:
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanPullup(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates,
                                const vector<string> &projectColumnNames);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanPushdown(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                  const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates,
                                  const vector<string> &projectColumnNames);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanCachingOnly(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                     const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates,
                                     const vector<string> &projectColumnNames);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanHybrid(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates,
                                const vector<string> &projectColumnNames);

  /**
   * Generate s3 select where clause from filter predicate
   * @param predicate
   * @return
   */
  string genFilterSql(const std::shared_ptr<Expression>& predicate);

  uint prePOpId_;
  shared_ptr<AWSClient> awsClient_;
  shared_ptr<Mode> mode_;
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_TRANSFORMER_PREPTOS3PTRANSFORMER_H
