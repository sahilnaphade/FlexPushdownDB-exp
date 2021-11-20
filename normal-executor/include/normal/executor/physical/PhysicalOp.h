//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PHYSICALOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PHYSICALOP_H

#include <normal/executor/message/Message.h>
#include <normal/executor/message/Envelope.h>
#include <normal/executor/physical/POpContext.h>
#include <normal/executor/physical/Forward.h>

#include <caf/all.hpp>
#include <string>
#include <memory>
#include <map>

namespace normal::executor::physical {

/**
 * Base class for physical operators
 */
class PhysicalOp {

private:
  std::string name_;
  std::string type_;
  std::vector<std::string> projectColumnNames_;
  long queryId_;
  std::shared_ptr<POpContext> opContext_;
  std::map<std::string, std::string> producers_;
  std::map<std::string, std::string> consumers_;

public:
  explicit PhysicalOp(std::string name,
                      std::string type,
                      std::vector<std::string> projectColumnNames,
                      long queryId);
  virtual ~PhysicalOp() = default;

  std::string &name();
  const std::string &getType() const;
  const std::vector<std::string> &getProjectColumnNames() const;
  long getQueryId() const;
  std::map<std::string, std::string> producers();
  std::map<std::string, std::string> consumers();
  std::shared_ptr<POpContext> ctx();

  void setName(const std::string &Name);
  void setProjectColumnNames(const std::vector<std::string> &projectColumnNames);
  virtual void produce(const std::shared_ptr<PhysicalOp> &op);
  virtual void consume(const std::shared_ptr<PhysicalOp> &op);
  void create(const std::shared_ptr<POpContext>& ctx);

  virtual void onReceive(const normal::executor::message::Envelope &msg) = 0;
  void destroyActor();
};

} // namespace

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PHYSICALOP_H
