//
// Created by matt on 6/5/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILTER_FILTERPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILTER_FILTERPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/cache/SegmentKey.h>
#include <normal/catalogue/Table.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/expression/Filter.h>
#include <normal/tuple/TupleSet.h>
#include <memory>

using namespace normal::executor::message;
using namespace normal::catalogue;

namespace normal::executor::physical::filter {

class FilterPOp : public PhysicalOp {
public:
  explicit FilterPOp(std::string name,
                  std::shared_ptr<normal::expression::gandiva::Expression> predicate,
                  std::shared_ptr<Table> table,
                  std::vector<std::string> projectColumnNames,
                  long queryId = 0,
                  std::vector<std::shared_ptr<normal::cache::SegmentKey>> weightedSegmentKeys = {});

  void onReceive(const Envelope &Envelope) override;

  [[nodiscard]] size_t getFilterTimeNS() const;
  [[nodiscard]] size_t getFilterInputBytes() const;
  [[nodiscard]] size_t getFilterOutputBytes() const;

private:
  shared_ptr<normal::expression::gandiva::Expression> predicate_;

  std::optional<std::shared_ptr<normal::expression::Filter>> filter_;

  /**
   * A buffer of received tuples not yet filtered
   */
  std::shared_ptr<normal::tuple::TupleSet> received_;

  /**
   * A buffer of filtered tuples not yet sent
   */
  std::shared_ptr<normal::tuple::TupleSet> filtered_;

  void onStart();
  void onTuple(const TupleMessage& Message);
  void onComplete(const CompleteMessage& Message);

  void bufferTuples(const std::shared_ptr<normal::tuple::TupleSet>& tupleSet);
  void buildFilter();
  void filterTuples();
  void sendTuples();
  void sendSegmentWeight();

  bool isApplicable(const std::shared_ptr<normal::tuple::TupleSet>& tupleSet);

  long totalNumRows_ = 0;
  long filteredNumRows_ = 0;
  size_t filterTimeNS_ = 0;
  size_t inputBytesFiltered_ = 0;
  size_t outputBytesFiltered_ = 0;

  /**
   * Whether all predicate columns are covered in the schema of received tuples
   */
  std::shared_ptr<bool> applicable_;

  /**
   * Used to compute filter weight, set to nullptr and {} if its producer is not table scan
   */
  std::shared_ptr<Table> table_;
  std::vector<std::shared_ptr<normal::cache::SegmentKey>> weightedSegmentKeys_;
};

inline bool recordSpeeds = false;
inline size_t totalBytesFiltered_ = 0;
}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILTER_FILTERPOP_H
