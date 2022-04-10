//
// Created by Yifei Yang on 4/4/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_BITMAPMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_BITMAPMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <caf/all.hpp>

namespace fpdb::executor::message {

/**
 * Message sent for filter bitmap pushdown
 */
class BitmapMessage: public Message {

public:
  explicit BitmapMessage(const std::optional<std::vector<bool>> &bitmap,
                         const std::optional<std::string> &receiverInFPDBStoreSuper,
                         const std::string &sender);
  BitmapMessage() = default;
  BitmapMessage(const BitmapMessage&) = default;
  BitmapMessage& operator=(const BitmapMessage&) = default;

  std::string getTypeString() const override;

  const std::optional<std::vector<bool>> &getBitmap() const;
  const std::optional<std::string> &getReceiverInFPDBStoreSuper() const;

private:
  // the bitmap, nullopt means the bitmap cannot be constructed
  std::optional<std::vector<bool>> bitmap_;

  // the operator inside FPDBStoreSuperPOp to receive the bitmap, only used when sending bitmap from compute to storage
  // but not the opposite
  std::optional<std::string> receiverInFPDBStoreSuper_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BitmapMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("bitmap", msg.bitmap_),
                                f.field("receiverInFPDBStoreSuper", msg.receiverInFPDBStoreSuper_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_BITMAPMESSAGE_H
