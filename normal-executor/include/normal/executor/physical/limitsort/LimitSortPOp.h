//
// Created by Yifei Yang on 12/6/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_LIMITSORTPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_LIMITSORTPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleMessage.h>
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

  void onReceive(const Envelope &msg) override;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);

  shared_ptr<TupleSet> makeInput(const shared_ptr<TupleSet> &tupleSet);

  shared_ptr<TupleSet> selectK(const shared_ptr<TupleSet> &tupleSet);

  arrow::compute::SelectKOptions selectKOptions_;
  optional<shared_ptr<TupleSet>> result_;

};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_LIMITSORTPOP_H
