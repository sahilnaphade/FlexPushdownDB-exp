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
#include <normal/tuple/TupleSet2.h>
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

private:
  shared_ptr<normal::expression::gandiva::Expression> predicate_;

  std::optional<std::shared_ptr<normal::expression::Filter>> filter_;

  /**
   * A buffer of received tuples not yet filtered
   */
  std::shared_ptr<normal::tuple::TupleSet2> received_;

  /**
   * A buffer of filtered tuples not yet sent
   */
  std::shared_ptr<normal::tuple::TupleSet2> filtered_;

  void onStart();
  void onTuple(const TupleMessage& Message);
  void onComplete(const CompleteMessage& Message);

  void bufferTuples(const std::shared_ptr<normal::tuple::TupleSet2>& tupleSet);
  void buildFilter();
  void filterTuples();
  void sendTuples();
  void sendSegmentWeight();

  /**
   * Catalogue Table
   */
  std::shared_ptr<Table> table_;

  /**
   * Whether all predicate columns are covered in the schema of received tuples
   */
  std::shared_ptr<bool> applicable_;
  bool isApplicable(const std::shared_ptr<normal::tuple::TupleSet2>& tupleSet);

  /**
   * Used to compute filter weight
   */
  std::vector<std::shared_ptr<normal::cache::SegmentKey>> weightedSegmentKeys_;
  long totalNumRows_ = 0;
  long filteredNumRows_ = 0;
};

inline bool recordSpeeds = false;
inline size_t totalBytesFiltered_ = 0;
}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILTER_FILTERPOP_H
