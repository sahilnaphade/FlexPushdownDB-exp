//
// Created by Yifei Yang on 4/4/22.
//

#include <fpdb/executor/message/BitmapMessage.h>

namespace fpdb::executor::message {

BitmapMessage::BitmapMessage(const std::optional<std::vector<bool>> &bitmap,
                             const std::optional<std::string> &receiverInFPDBStoreSuper,
                             const std::string &sender) :
  Message(BITMAP, sender),
  bitmap_(bitmap),
  receiverInFPDBStoreSuper_(receiverInFPDBStoreSuper) {}

std::string BitmapMessage::getTypeString() const {
  return "BitmapMessage";
}

const std::optional<std::vector<bool>> &BitmapMessage::getBitmap() const {
  return bitmap_;
}

const std::optional<std::string> &BitmapMessage::getReceiverInFPDBStoreSuper() const {
  return receiverInFPDBStoreSuper_;
}

}
