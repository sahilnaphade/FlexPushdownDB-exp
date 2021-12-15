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
           const vector<string> &projectColumnNames);

  void onReceive(const Envelope &msg) override;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);
  void produce(const shared_ptr<PhysicalOp> &op) override;

  tl::expected<void, string> splitAndSend();
  tl::expected<void, string> bufferInput(const shared_ptr<TupleSet>& tupleSet);
  tl::expected<vector<shared_ptr<TupleSet>>, string> split();
  void send(const vector<shared_ptr<TupleSet>> &tupleSets);
  void clear();

  vector<string> consumers_;
  optional<shared_ptr<TupleSet>> inputTupleSet_;

};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SPLIT_SPLITPOP_H
