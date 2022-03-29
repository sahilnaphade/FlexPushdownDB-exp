//
// Created by Yifei Yang on 3/1/22.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_FPDB_STORE_FPDBSTORECONNECTOR_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_FPDB_STORE_FPDBSTORECONNECTOR_H

#include <fpdb/catalogue/obj-store/ObjStoreConnector.h>
#include <string>

namespace fpdb::catalogue::obj_store {

class FPDBStoreConnector: public ObjStoreConnector {

public:
  FPDBStoreConnector(const std::string &host,
                     int fileServicePort,
                     int flightPort);
  FPDBStoreConnector() = default;
  FPDBStoreConnector(const FPDBStoreConnector&) = default;
  FPDBStoreConnector& operator=(const FPDBStoreConnector&) = default;
  ~FPDBStoreConnector() = default;

  const std::string &getHost() const;
  int getFileServicePort() const;
  int getFlightPort() const;

private:
  std::string host_;
  int fileServicePort_;
  int flightPort_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, FPDBStoreConnector& conn) {
    return f.object(conn).fields(f.field("storeType", conn.storeType_),
                                 f.field("host", conn.host_),
                                 f.field("fileServicePort", conn.fileServicePort_),
                                 f.field("flightPort", conn.flightPort_));
  }
};

}


#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_FPDB_STORE_FPDBSTORECONNECTOR_H
