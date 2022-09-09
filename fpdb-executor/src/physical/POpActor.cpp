//
// Created by Matt Youill on 31/12/19.
//

#include <fpdb/executor/physical/POpActor.h>
#include <fpdb/executor/message/StartMessage.h>
#include <fpdb/executor/message/ConnectMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <arrow/flight/api.h>
#include <spdlog/spdlog.h>
#include <utility>

namespace fpdb::executor::physical {

POpActor::POpActor(::caf::actor_config &cfg, std::shared_ptr<PhysicalOp> opBehaviour) :
	::caf::event_based_actor(cfg),
	opBehaviour_(std::move(opBehaviour)) {

  name_ = opBehaviour_->name();
}

// TODO: add TupleSetReadyFPDBStoreMessage
::caf::behavior behaviour(POpActor *self) {

  auto ctx = self->operator_()->ctx();
  ctx->operatorActor(self);

  return {
	  [=](GetProcessingTimeAtom) {
		auto start = std::chrono::steady_clock::now();
		auto finish = std::chrono::steady_clock::now();
		auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
		self->incrementProcessingTime(elapsedTime);
		return self->getProcessingTime();
	  },
	  [=](const fpdb::executor::message::Envelope &msg) {

		auto start = std::chrono::steady_clock::now();

    SPDLOG_DEBUG("Message received  |  recipient: '{}', sender: '{}', type: '{}'",
                 self->operator_()->name(),
                 msg.message().sender(),
                 msg.message().type());

		if (msg.message().type() == MessageType::CONNECT) {
		  auto connectMessage = dynamic_cast<const message::ConnectMessage &>(msg.message());

		  for (const auto &element: connectMessage.connections()) {
        auto localEntry = LocalPOpDirectoryEntry(element.getName(),
                                element.getActorHandle(),
                                element.getConnectionType(),
                                false);

        auto result = self->operator_()->ctx()->operatorMap().insert(localEntry);
        if (!result.has_value()) {
          self->operator_()->ctx()->notifyError(result.error());
        }
		  }
		}
		else if (msg.message().type() == MessageType::START) {
		  auto startMessage = dynamic_cast<const message::StartMessage &>(msg.message());

		  self->running_ = true;

		  self->operator_()->onReceive(msg);

      while (!self->messageBuffer_.empty()){
        const auto &bufferedMsg = self->messageBuffer_.front();
        self->on_regular_message(bufferedMsg);
        self->messageBuffer_.pop();

        // if running_ turns to false, we should not continue processing rest messages in buffer
        if (!self->running_) {
          break;
        }
		  }

		} else if (msg.message().type() == MessageType::STOP) {
		  self->running_ = false;

		  self->operator_()->onReceive(msg);
		}
		else{
		  if (!self->running_){
			  self->messageBuffer_.emplace(msg);
		  }
		  else {
        self->on_regular_message(msg);
		  }
		}

		auto finish = std::chrono::steady_clock::now();
		auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
		self->incrementProcessingTime(elapsedTime);
	  }
  };
}

void POpActor::on_regular_message(const fpdb::executor::message::Envelope &msg) {
  if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const message::CompleteMessage &>(msg.message());
    auto result = opBehaviour_->ctx()->operatorMap().setComplete(msg.message().sender());
    if (!result.has_value()) {
      opBehaviour_->ctx()->notifyError(result.error());
    }
  }

  opBehaviour_->onReceive(msg);
}

::caf::behavior POpActor::make_behavior() {
  return behaviour(this);
}

std::shared_ptr<fpdb::executor::physical::PhysicalOp> POpActor::operator_() const {
  return opBehaviour_;
}

long POpActor::getProcessingTime() const {
  return processingTime_;
}

void POpActor::incrementProcessingTime(long time) {
  processingTime_ += time;
}

void POpActor::on_exit() {
  SPDLOG_DEBUG("Stopping operator  |  name: '{}'", this->opBehaviour_->name());

  /*
   * Need to delete the actor handle in operator otherwise CAF will never release the actor
   */
  this->opBehaviour_->destroyActor();
}

}
