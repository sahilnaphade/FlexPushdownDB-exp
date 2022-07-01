//
// Created by Yifei Yang on 6/26/22.
//

#include <fpdb/executor/FPDBStoreExecution.h>

namespace fpdb::executor {

FPDBStoreExecution::FPDBStoreExecution(long queryId,
                                       const std::shared_ptr<::caf::actor_system> &actorSystem,
                                       const std::shared_ptr<PhysicalPlan> &physicalPlan):
  Execution(queryId, actorSystem, {}, nullptr, physicalPlan, false) {}

void FPDBStoreExecution::join() {
  SPDLOG_DEBUG("Waiting for all operators to complete");

  auto handle_err = [&](const ::caf::error &err) {
    throw runtime_error(to_string(err));
  };

  bool allComplete = false;
  (*rootActor_)->receive_while([&] { return !allComplete; })(
          [&](const Envelope &e) {
            const auto &msg = e.message();
            SPDLOG_DEBUG("Query root actor received message  |  query: '{}', messageKind: '{}', from: '{}'",
                         queryId_, msg.getTypeString(), msg.sender());

            auto errAct = [&](const std::string &errMsg) {
              allComplete = true;
              close();
              throw runtime_error(errMsg);
            };

            switch (msg.type()) {
              case MessageType::COMPLETE: {
                this->opDirectory_.setComplete(msg.sender())
                        .map_error(errAct);
                allComplete = this->opDirectory_.allComplete();
                break;
              }

              case MessageType::BITMAP: {
                auto bitmapMessage = ((BitmapMessage &) msg);
                bitmaps_[bitmapMessage.sender()] = bitmapMessage.getBitmap();
                break;
              }

              case MessageType::TUPLESET_BUFFER: {
                auto tupleSetBufferMessage = ((TupleSetBufferMessage &) msg);
                auto consumer = tupleSetBufferMessage.getConsumer();
                auto tupleSetIt = tupleSets_.find(consumer);
                if (tupleSetIt == tupleSets_.end()) {
                  tupleSets_[consumer] = tupleSetBufferMessage.tuples();
                } else {
                  auto res = tupleSetIt->second->append(tupleSetBufferMessage.tuples());
                  if (!res.has_value()) {
                    errAct(res.error());
                  }
                }
              }

#if SHOW_DEBUG_METRICS == true
              case MessageType::DEBUG_METRICS: {
                auto debugMetricsMsg = ((DebugMetricsMessage &) msg);
                debugMetrics_.addBytesFromStore(debugMetricsMsg.getBytesFromStore());
                break;
              }
#endif

              case MessageType::ERROR: {
                errAct(fmt::format("ERROR: {}, from {}", ((ErrorMessage &) msg).getContent(), msg.sender()));
              }
              default: {
                errAct(fmt::format("Invalid message type sent to the root actor: {}, from {}", msg.getTypeString(), msg.sender()));
              }
            }

          },
          handle_err);

  stopTime_ = chrono::steady_clock::now();
}

const std::unordered_map<std::string, std::shared_ptr<TupleSet>> &FPDBStoreExecution::getTupleSets() const {
  return tupleSets_;
}

const std::unordered_map<std::string, std::vector<int64_t>> &FPDBStoreExecution::getBitmaps() const {
  return bitmaps_;
}

}
