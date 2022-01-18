//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PHYSICALOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PHYSICALOP_H

#include <normal/executor/physical/Forward.h>
#include <normal/executor/physical/POpContext.h>
#include <normal/executor/physical/POpType.h>
#include <normal/executor/message/Message.h>
#include <normal/executor/message/Envelope.h>
#include <caf/all.hpp>
#include <string>
#include <memory>
#include <map>

namespace normal::executor::physical {

/**
 * Base class for physical operators
 */
class PhysicalOp {

public:
  explicit PhysicalOp(std::string name,
                      POpType type,
                      std::vector<std::string> projectColumnNames,
                      int nodeId);
  PhysicalOp() = default;
  PhysicalOp(const PhysicalOp&) = default;
  PhysicalOp& operator=(const PhysicalOp&) = default;
  virtual ~PhysicalOp() = default;

  // getters
  std::string &name();
  POpType getType() const;
  virtual std::string getTypeString() const = 0;
  const std::vector<std::string> &getProjectColumnNames() const;
  int getNodeId() const;
  long getQueryId() const;
  std::set<std::string> producers();
  std::set<std::string> consumers();
  std::shared_ptr<POpContext> ctx();

  // setters
  void setName(const std::string &Name);
  void setProjectColumnNames(const std::vector<std::string> &projectColumnNames);
  void setQueryId(long queryId);
  virtual void produce(const std::shared_ptr<PhysicalOp> &op);
  virtual void consume(const std::shared_ptr<PhysicalOp> &op);
  void create(const std::shared_ptr<POpContext>& ctx);

  virtual void onReceive(const normal::executor::message::Envelope &msg) = 0;
  void destroyActor();

protected:
  std::string name_;
  POpType type_;
  std::vector<std::string> projectColumnNames_;
  int nodeId_;
  long queryId_;
  std::shared_ptr<POpContext> opContext_;
  std::set<std::string> producers_;
  std::set<std::string> consumers_;

};

} // namespace

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PHYSICALOP_H
