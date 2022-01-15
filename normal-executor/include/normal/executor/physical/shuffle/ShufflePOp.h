//
// Created by matt on 17/6/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/tuple/TupleSet.h>

using namespace normal::executor::message;
using namespace normal::tuple;
using namespace std;

namespace normal::executor::physical::shuffle {

/**
 * A firts cut of a shuffle operator, shuffles on a single key only
 */
class ShufflePOp : public PhysicalOp {

public:
  ShufflePOp(string name,
             vector<string> columnNames,
             vector<string> projectColumnNames);
  ShufflePOp() = default;
  ShufflePOp(const ShufflePOp&) = default;
  ShufflePOp& operator=(const ShufflePOp&) = default;

  /**
   * Operators message handler
   * @param msg
   */
  void onReceive(const Envelope &msg) override;

private:
  /**
   * Start message handler
   */
  void onStart();

  /**
   * Completion message handler
   */
  void onComplete(const CompleteMessage &);

  /**
   * Tuples message handler
   * @param message
   */
  void onTuple(const TupleMessage &message);

  /**
   * Set the producer operator
   * @param operator_
   */
  void produce(const shared_ptr<PhysicalOp> &operator_) override;

  /**
   * Adds the tuple set to the outbound buffer for the given slot
   * @param tupleSet
   * @param partitionIndex
   * @return
   */
  [[nodiscard]] tl::expected<void, string> buffer(const shared_ptr<TupleSet>& tupleSet, int partitionIndex);

  /**
   * Sends the buffered tupleset if its big enough or force is true
   * @param partitionIndex
   * @param force
   * @return
   */
  [[nodiscard]] tl::expected<void, string> send(int partitionIndex, bool force);

  vector<string> columnNames_;
  vector<string> consumers_;
  vector<optional<shared_ptr<TupleSet>>> buffers_;
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEPOP_H
