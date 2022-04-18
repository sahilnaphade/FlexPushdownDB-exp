//
// Created by matt on 4/2/22.
//

#include "fpdb/store/server/flight/CmdType.hpp"
#include "fpdb/store/server/flight/Util.hpp"

namespace fpdb::store::server::flight {

CmdType::CmdType(CmdTypeId id, std::string name) : id_(id), name_(std::move(name)) {
}

CmdTypeId CmdType::id() const {
  return id_;
}

const std::string& CmdType::name() const {
  return name_;
}

std::shared_ptr<CmdType> CmdType::get_object() {
  return std::make_shared<CmdType>(CmdTypeId::GET_OBJECT, GetObjectCmdTypeName.data());
}

std::shared_ptr<CmdType> CmdType::select_object_content() {
  return std::make_shared<CmdType>(CmdTypeId::SELECT_OBJECT_CONTENT, SelectObjectContentCmdTypeName.data());
}

std::shared_ptr<CmdType> CmdType::put_bitmap() {
  return std::make_shared<CmdType>(CmdTypeId::PUT_BITMAP, PutBitmapCmdTypeName.data());
}

} // namespace fpdb::store::server::flight
