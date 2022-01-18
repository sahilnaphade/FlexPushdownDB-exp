//
// Created by Yifei Yang on 12/13/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SPLIT_SPLITPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SPLIT_SPLITPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/tuple/TupleSet.h>

using namespace normal::executor::message;
using namespace normal::tuple;
using namespace std;

namespace normal::executor::physical::split {

class SplitPOp : public PhysicalOp {

public:
  SplitPOp(const string &name,
           const vector<string> &projectColumnNames,
           int nodeId);
  SplitPOp() = default;
  SplitPOp(const SplitPOp&) = default;
  SplitPOp& operator=(const SplitPOp&) = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void produce(const shared_ptr<PhysicalOp> &op) override;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);

  tl::expected<void, string> splitAndSend();
  tl::expected<void, string> bufferInput(const shared_ptr<TupleSet>& tupleSet);
  tl::expected<vector<shared_ptr<TupleSet>>, string> split();
  void send(const vector<shared_ptr<TupleSet>> &tupleSets);
  void clear();

  vector<string> consumerVec_;
  std::optional<shared_ptr<TupleSet>> inputTupleSet_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SplitPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("consumerVec", op.consumerVec_));
  }
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SPLIT_SPLITPOP_H
