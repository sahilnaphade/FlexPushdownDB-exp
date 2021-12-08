//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_COLLATE_COLLATEPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_COLLATE_COLLATEPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/physical/POpContext.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/tuple/TupleSet.h>
#include <arrow/api.h>
#include <string>
#include <memory>                  // for unique_ptr

namespace normal::tuple {
	class TupleSet;
}

namespace normal::executor::physical::collate {

class CollatePOp : public PhysicalOp {

public:
  explicit CollatePOp(std::string name,
                      std::vector<std::string> projectColumnNames);
  ~CollatePOp() override = default;

  std::shared_ptr<TupleSet> tuples();

  void onReceive(const normal::executor::message::Envelope &message) override;

private:
  void onStart();
  void onComplete(const normal::executor::message::CompleteMessage &message);
  void onTuple(const normal::executor::message::TupleMessage& message);

  std::shared_ptr<TupleSet> tuples_;
  std::vector<std::shared_ptr<arrow::Table>> tables_;
  size_t tablesCutoff_ = 20;
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_COLLATE_COLLATEPOP_H
