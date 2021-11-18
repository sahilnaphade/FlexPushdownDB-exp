//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_COLLATE_COLLATE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_COLLATE_COLLATE_H

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

class Collate : public PhysicalOp {

private:
  std::shared_ptr<TupleSet> tuples_;
private:
  std::vector<std::shared_ptr<arrow::Table>> tables_;
  size_t tablesCutoff_ = 20;
  void onStart();

  void onComplete(const normal::executor::message::CompleteMessage &message);
  void onTuple(const normal::executor::message::TupleMessage& message);
  void onReceive(const normal::executor::message::Envelope &message) override;

public:
  explicit Collate(std::string name, long queryId = 0);
  ~Collate() override = default;
  void show();
  std::shared_ptr<TupleSet> tuples();
  [[maybe_unused]] void setTuples(const std::shared_ptr<TupleSet> &Tuples);
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_COLLATE_COLLATE_H
