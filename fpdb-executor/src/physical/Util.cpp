//
// Created by Yifei Yang on 2/22/22.
//

#include <fpdb/executor/physical/Util.h>

namespace fpdb::executor::physical {
  
void Util::connectOneToOne(vector<shared_ptr<PhysicalOp>> &producers,
                           vector<shared_ptr<PhysicalOp>> &consumers) {
  if (producers.size() != consumers.size()) {
    throw runtime_error(fmt::format("Bad one-to-one operator connection input, producers has {}, but consumers has {}",
                                    producers.size(), consumers.size()));
  }
  for (size_t i = 0; i < producers.size(); ++i) {
    producers[i]->produce(consumers[i]);
    consumers[i]->consume(producers[i]);
  }
}

void Util::connectManyToMany(vector<shared_ptr<PhysicalOp>> &producers,
                             vector<shared_ptr<PhysicalOp>> &consumers) {
  for (const auto &producer: producers) {
    for (const auto &consumer: consumers) {
      producer->produce(consumer);
      consumer->consume(producer);
    }
  }
}

void Util::connectManyToOne(vector<shared_ptr<PhysicalOp>> &producers,
                            shared_ptr<PhysicalOp> &consumer) {
  for (const auto &producer: producers) {
    producer->produce(consumer);
    consumer->consume(producer);
  }
}
  
}
