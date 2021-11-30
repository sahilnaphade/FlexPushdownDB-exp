//
// Created by matt on 5/12/19.
//

#include <normal/executor/physical/POpContext.h>
#include <normal/executor/message/Message.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/cache/LoadRequestMessage.h>
#include <normal/executor/cache/SegmentCacheActor.h>
#include <normal/executor/message/cache/CacheMetricsMessage.h>
#include <spdlog/spdlog.h>
#include <utility>
#include <cassert>

using namespace normal::executor::message;

namespace normal::executor::physical {

void POpContext::tell(std::shared_ptr<Message> &msg) {

  assert(this);

  if(complete_)
	throw std::runtime_error(fmt::format("Cannot tell message to consumers, operator {} ('{}') is complete", this->operatorActor()->id(), this->operatorActor()->operator_()->name()));

  message::Envelope e(msg);

  // Send message to consumers
  for(const auto& consumer: this->operatorActor()->operator_()->consumers()){
    caf::actor actorHandle = operatorMap_.get(consumer.first).value().getActor();
    operatorActor_->anon_send(actorHandle, e);
  }
}

tl::expected<void, std::string> POpContext::send(const std::shared_ptr<message::Message> &msg, const std::string& recipientId) {

  message::Envelope e(msg);

  if(recipientId == "SegmentCache"){
    if(msg->type() == "LoadRequestMessage"){
      operatorActor_->request(segmentCacheActor_, infinite, normal::executor::cache::LoadAtom::value, std::static_pointer_cast<normal::executor::message::LoadRequestMessage>(msg))
      .then([=](const std::shared_ptr<normal::executor::message::LoadResponseMessage>& response){
      operatorActor_->anon_send(this->operatorActor(), Envelope(response));
      });
    }
    else if(msg->type() == "StoreRequestMessage"){
      operatorActor_->anon_send(segmentCacheActor_, normal::executor::cache::StoreAtom::value, std::static_pointer_cast<normal::executor::message::StoreRequestMessage>(msg));
    }
    else if(msg->type() == "WeightRequestMessage"){
      operatorActor_->anon_send(segmentCacheActor_, normal::executor::cache::WeightAtom::value, std::static_pointer_cast<normal::executor::message::WeightRequestMessage>(msg));
    }
    else if(msg->type() == "CacheMetricsMessage"){
      operatorActor_->anon_send(segmentCacheActor_, normal::executor::cache::MetricsAtom::value, std::static_pointer_cast<normal::executor::message::CacheMetricsMessage>(msg));
    }
    else{
      throw std::runtime_error("Unrecognized message " + msg->type());
    }

	return {};
  }

  auto expectedOperator = operatorMap_.get(recipientId);
  if(expectedOperator.has_value()){
    auto recipientOperator = expectedOperator.value();
	operatorActor_->anon_send(recipientOperator.getActor(), e);
	return {};
  }
  else{
  	return tl::unexpected(fmt::format("Actor with id '{}' not found", recipientId));
  }
}

POpContext::POpContext(caf::actor rootActor, caf::actor segmentCacheActor):
    operatorActor_(nullptr),
    rootActor_(std::move(rootActor)),
    segmentCacheActor_(std::move(segmentCacheActor))
{}

LocalPOpDirectory &POpContext::operatorMap() {
  return operatorMap_;
}
POpActor* POpContext::operatorActor() {
  return operatorActor_;
}
void POpContext::operatorActor(POpActor *operatorActor) {
  operatorActor_ = operatorActor;
}

/**
 * Sends a CompleteMessage to all consumers and the root actor
 */
void POpContext::notifyComplete() {

  SPDLOG_DEBUG("Completing operator  |  source: {} ('{}')", this->operatorActor()->id(), this->operatorActor()->operator_()->name());
  if(complete_)
    throw std::runtime_error(fmt::format("Cannot complete already completed operator {} ('{}')", this->operatorActor()->id(), this->operatorActor()->operator_()->name()));

  POpActor* operatorActor = this->operatorActor();

  std::shared_ptr<message::Message> msg = std::make_shared<message::CompleteMessage>(operatorActor->operator_()->name());
  message::Envelope e(msg);

  // Send message to consumers
  for(const auto& consumer: this->operatorActor()->operator_()->consumers()){
    caf::actor actorHandle = operatorMap_.get(consumer.first).value().getActor();
    operatorActor->anon_send(actorHandle, e);
  }

  // Send message to root actor
  operatorActor->anon_send(rootActor_, e);

  complete_ = true;
}

bool POpContext::isComplete() const {
  return complete_;
}

void POpContext::destroyActorHandles() {
  operatorMap_.destroyActorHandles();
  destroy(rootActor_);
  destroy(segmentCacheActor_);
}

} // namespace
