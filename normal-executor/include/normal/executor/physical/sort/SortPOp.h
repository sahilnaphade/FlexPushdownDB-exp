//
// Created by Yifei Yang on 11/20/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_SORTPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_SORTPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/tuple/serialization/ArrowSerializer.h>
#include <arrow/compute/api.h>

using namespace normal::executor::message;
using namespace std;

namespace normal::executor::physical::sort {

class SortPOp : public PhysicalOp {

public:
  SortPOp(const string &name,
          const arrow::compute::SortOptions &sortOptions,
          const vector<string> &projectColumnNames);
  SortPOp() = default;
  SortPOp(const SortPOp&) = default;
  SortPOp& operator=(const SortPOp&) = default;

  void onReceive(const Envelope &msg) override;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);

  void buffer(const shared_ptr<TupleSet> &tupleSet);
  shared_ptr<TupleSet> sort();

  arrow::compute::SortOptions sortOptions_;
  std::optional<shared_ptr<TupleSet>> buffer_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SortPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("sortOptions", op.sortOptions_));
  }
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_SORTPOP_H
