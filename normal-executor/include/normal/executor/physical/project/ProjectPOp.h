//
// Created by matt on 14/4/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PROJECT_PROJECTPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PROJECT_PROJECTPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/expression/Expression.h>
#include <normal/expression/Projector.h>

using namespace normal::executor::message;

namespace normal::executor::physical::project {

class ProjectPOp : public PhysicalOp {

public:

  /**
   * Constructor
   * @param Name Descriptive name
   * @param Expressions Expressions to evaluate to produce the attributes in the projection
   */
  ProjectPOp(const std::string &name,
          std::vector<std::shared_ptr<normal::expression::gandiva::Expression>> exprs,
          std::vector<std::string> exprNames,
          long queryId = 0);

  /**
   * Default destructor
   */
  ~ProjectPOp() override = default;

  /**
   * Operators message handler
   * @param msg
   */
  void onReceive(const Envelope &msg) override;

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

private:
  /**
   * Build the projector from the input schema
   * @param inputSchema
   */
  void buildProjector(const TupleMessage &message);

  /**
   * Adds the tuples in the tuple message to the internal buffer
   * @param message
   */
  void bufferTuples(const TupleMessage &message);

  /**
   * Sends the given projected tuples to consumers
   * @param projected
   */
  void sendTuples(std::shared_ptr<TupleSet> &projected);

  /**
   * Projects the tuples and sends them to consumers
   */
  void projectAndSendTuples();

  /**
   * The project expressions and the attribute names
   */
  std::vector<std::shared_ptr<normal::expression::gandiva::Expression>> exprs_;
  std::vector<std::string> exprNames_;

  /**
   * A buffer of received tuples that are not projected until enough tuples have been received
   */
  std::shared_ptr<TupleSet> tuples_;

  /**
   * The expression projector, created when input schema is extracted from first tuple received
   */
  std::optional<std::shared_ptr<normal::expression::Projector>> projector_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PROJECT_PROJECTPOP_H
