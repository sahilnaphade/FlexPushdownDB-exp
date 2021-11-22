//
// Created by Yifei Yang on 2/9/21.
//

#ifndef NORMAL_UTIL_UTIL_H
#define NORMAL_UTIL_UTIL_H

#include <normal/util/Globals.h>
#include <vector>
#include <unordered_map>

namespace normal::util {
  /**
   * Equivalent to java instanceof
   * @tparam Base
   * @tparam T
   * @return
   */
  template<typename Base, typename T>
  inline bool instanceof(const T&) {
    return std::is_base_of<Base, T>::value;
  }

  /**
   * File utils
   * @param filePath
   * @return
   */
  std::string readFile(const std::string& filePath);
  std::vector<std::string> readFileByLine(const std::string& filePath);

  /**
   * Config utils
   * @param fileName
   * @return
   */
  std::unordered_map<std::string, std::string> readConfig(const std::string &fileName);

  /**
   * Given a start and finish number, will create pairs of numbers from start to finish (inclusive)
   * evenly split across the number of ranges given.
   *
   * E.g.
   * ranges(0,8,3) -> [[0,1,2][3,4,5][6,7,8]]
   * ranges(0,9,3) -> [[0,1,2,3][4,5,6,7][8,9]]
   * ranges(0,10,3) -> [[0,1,2,3][4,5,6,7][8,9,10]]
   *
   * @tparam T
   * @param start
   * @param finish
   * @param numRanges
   * @return
   */
  template<typename T>
  static std::vector<std::pair<T, T>> ranges(T start, T finish, T numRanges) {
    std::vector<std::pair<T, T>> result;

    T rangeSize = ((finish - start) / numRanges) + 1;

    for (int i = 0; i < numRanges; ++i) {
      T rangeStart = rangeSize * i;
      T rangeStop = std::min((rangeStart + rangeSize) - 1, finish);
      result.push_back(std::pair{rangeStart, rangeStop});
    }

    return result;
  }

  bool isInteger(const std::string& str);
  std::string getLocalIp();
}


#endif //NORMAL_UTIL_UTIL_H
