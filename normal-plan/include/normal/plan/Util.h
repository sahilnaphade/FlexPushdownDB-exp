//
// Created by Yifei Yang on 12/17/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_UTIL_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_UTIL_H

#include <set>
#include <unordered_map>
#include <memory>
#include <string>

using namespace std;

namespace normal::plan {

class Util {

public:
  static void renameColumns(set<string> &columnNames, const unordered_map<string, string> &renames);
  static void deRenameColumns(set<string> &renamedColumnNames, const unordered_map<string, string> &renames);

};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_UTIL_H
