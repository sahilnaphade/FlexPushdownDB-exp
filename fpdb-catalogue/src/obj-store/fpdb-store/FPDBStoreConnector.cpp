//
// Created by Yifei Yang on 3/1/22.
//

#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>

namespace fpdb::catalogue::obj_store {

FPDBStoreConnector::FPDBStoreConnector(const std::string &host,
                                       int fileServicePort,
                                       int flightPort):
  ObjStoreConnector(ObjStoreType::FPDB_STORE),
  host_(host),
  fileServicePort_(fileServicePort),
  flightPort_(flightPort) {}

const std::string &FPDBStoreConnector::getHost() const {
  return host_;
}

int FPDBStoreConnector::getFileServicePort() const {
  return fileServicePort_;
}

int FPDBStoreConnector::getFlightPort() const {
  return flightPort_;
}

}
