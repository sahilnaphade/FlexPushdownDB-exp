//
// Created by Yifei Yang on 3/7/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FPDBSTORESERVERCONFIG_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FPDBSTORESERVERCONFIG_HPP

#include <string>

namespace fpdb::store::server {

/**
 * fpdb-store-server config directly parsed from config file
 */
class FPDBStoreServerConfig {

public:
  FPDBStoreServerConfig(int fileServicePort,
                        int flightPort,
                        const std::string &storeRootPath);

  static std::shared_ptr<FPDBStoreServerConfig> parseFPDBStoreServerConfig();

  int getFileServicePort() const;
  int getFlightPort() const;
  const std::string &getStoreRootPath() const;

private:
  int fileServicePort_;
  int flightPort_;
  std::string storeRootPath_;

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FPDBSTORESERVERCONFIG_HPP
