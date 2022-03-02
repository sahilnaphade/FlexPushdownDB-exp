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
                     int port);

  const std::string &getHost() const;
  int getPort() const;

private:
  std::string host_;
  int port_;

};

}


#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_FPDB_STORE_FPDBSTORECONNECTOR_H
