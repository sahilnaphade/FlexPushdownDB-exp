//
// Created by Yifei Yang on 2/9/21.
//

#ifndef NORMAL_UTIL_UTIL_H
#define NORMAL_UTIL_UTIL_H

#include <normal/util/Globals.h>
#include <vector>
#include <unordered_map>

namespace normal::util {
  template<typename Base, typename T>
  inline bool instanceof(const T&) {
    return std::is_base_of<Base, T>::value;
  }

  std::string readFile(const std::string& filePath);
  std::vector<std::string> readFileByLine(const std::string& filePath);
  std::unordered_map<std::string, std::string> readConfig(const std::string &fileName);

  bool isInteger(const std::string& str);
  std::string getLocalIp();
}


#endif //NORMAL_UTIL_UTIL_H
