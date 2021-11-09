//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_S3SELECTCONNECTOR_H
#define NORMAL_NORMAL_SQL_SRC_S3SELECTCONNECTOR_H

#include "../../../../ATTIC/Connector.h"

namespace normal::connector::s3 {

class S3SelectConnector : public normal::connector::Connector {

public:
  explicit S3SelectConnector(const std::string &name);
  ~S3SelectConnector() override = default;

};

}

#endif //NORMAL_NORMAL_SQL_SRC_S3SELECTCONNECTOR_H
