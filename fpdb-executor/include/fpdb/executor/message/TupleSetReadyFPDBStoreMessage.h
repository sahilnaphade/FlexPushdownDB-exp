//
// Created by Yifei Yang on 7/3/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETREADYFPDBSTOREMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETREADYFPDBSTOREMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <memory>

namespace fpdb::executor::message {

/**
 * Message denoting the input tupleSet is ready at FPDB-Store
 */
class TupleSetReadyFPDBStoreMessage : public Message {

public:
  explicit TupleSetReadyFPDBStoreMessage(const std::string &host, int port, const std::string &sender);
  TupleSetReadyFPDBStoreMessage() = default;
  TupleSetReadyFPDBStoreMessage(const TupleSetReadyFPDBStoreMessage&) = default;
  TupleSetReadyFPDBStoreMessage& operator=(const TupleSetReadyFPDBStoreMessage&) = default;
  ~TupleSetReadyFPDBStoreMessage() override = default;

  std::string getTypeString() const override;

  const std::string &getHost() const;
  int getPort() const;

private:
  std::string host_;
  int port_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleSetReadyFPDBStoreMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("host", msg.host_),
                                f.field("port", msg.port_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETREADYFPDBSTOREMESSAGE_H
