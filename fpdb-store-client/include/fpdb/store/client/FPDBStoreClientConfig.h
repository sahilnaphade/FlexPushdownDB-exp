//
// Created by Yifei Yang on 3/7/22.
//

#ifndef FPDB_FPDB_STORE_CLIENT_INCLUDE_FPDB_STORE_CLIENT_FPDBSTORECLIENTCONFIG_H
#define FPDB_FPDB_STORE_CLIENT_INCLUDE_FPDB_STORE_CLIENT_FPDBSTORECLIENTCONFIG_H

#include <string>

namespace fpdb::store::client {

class FPDBStoreClientConfig {

public:
  FPDBStoreClientConfig(const std::string &host,
                  int fileServicePort,
                  int flightPort);

  static std::shared_ptr<FPDBStoreClientConfig> parseFPDBStoreClientConfig();

  const std::string &getHost() const;
  int getFileServicePort() const;
  int getFlightPort() const;

private:
  std::string host_;
  int fileServicePort_;
  int flightPort_;

};

}


#endif //FPDB_FPDB_STORE_CLIENT_INCLUDE_FPDB_STORE_CLIENT_FPDBSTORECLIENTCONFIG_H
