//
// Created by Yifei Yang on 3/1/22.
//

#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>

namespace fpdb::catalogue::obj_store {

FPDBStoreConnector::FPDBStoreConnector(const std::string &host,
                                       int port):
  ObjStoreConnector(ObjStoreType::FPDB_STORE),
  host_(host),
  port_(port) {}

const std::string &FPDBStoreConnector::getHost() const {
  return host_;
}

int FPDBStoreConnector::getPort() const {
  return port_;
}

}
