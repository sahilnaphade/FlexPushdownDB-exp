//
// Created by Yifei Yang on 10/30/21.
//

#ifndef NORMAL_NORMAL_CALCITE_CPP_INCLUDE_NORMAL_CALCITE_CALCITECONFIG_H
#define NORMAL_NORMAL_CALCITE_CPP_INCLUDE_NORMAL_CALCITE_CALCITECONFIG_H

#include <string>

using namespace std;

namespace normal::calcite {

class CalciteConfig {
public:
  CalciteConfig(int port, string jarName);

  static std::shared_ptr<CalciteConfig> parseCalciteConfig();

  int getPort() const;
  const string &getJarName() const;

private:
  int port_;
  string jarName_;
};

}


#endif //NORMAL_NORMAL_CALCITE_CPP_INCLUDE_NORMAL_CALCITE_CALCITECONFIG_H
