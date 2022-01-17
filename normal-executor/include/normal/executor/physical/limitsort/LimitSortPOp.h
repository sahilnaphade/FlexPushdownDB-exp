//
// Created by Yifei Yang on 12/6/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_LIMITSORTPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_LIMITSORTPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/tuple/serialization/ArrowSerializer.h>
#include <arrow/compute/api.h>
#include <memory>

using namespace normal::executor::message;
using namespace std;

namespace normal::executor::physical::limitsort {

class LimitSortPOp : public PhysicalOp {

public:
  LimitSortPOp(const string &name,
               const arrow::compute::SelectKOptions &selectKOptions,
               const vector<string> &projectColumnNames);
  LimitSortPOp() = default;
  LimitSortPOp(const LimitSortPOp&) = default;
  LimitSortPOp& operator=(const LimitSortPOp&) = default;

  void onReceive(const Envelope &msg) override;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);

  shared_ptr<TupleSet> makeInput(const shared_ptr<TupleSet> &tupleSet);

  shared_ptr<TupleSet> selectK(const shared_ptr<TupleSet> &tupleSet);

  arrow::compute::SelectKOptions selectKOptions_;
  std::optional<shared_ptr<TupleSet>> result_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LimitSortPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("selectKOptions", op.selectKOptions_));
  }
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_LIMITSORTPOP_H
