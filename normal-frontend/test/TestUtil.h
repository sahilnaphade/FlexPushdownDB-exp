//
// Created by Yifei Yang on 11/30/21.
//

#ifndef NORMAL_NORMAL_FRONTEND_TEST_TESTUTIL_H
#define NORMAL_NORMAL_FRONTEND_TEST_TESTUTIL_H

#include <memory>
#include <vector>

using namespace std;

namespace normal::frontend::test {

class TestUtil {

public:
  /**
   * Test with calcite server already started, using pullup by default
   * @param schemaName
   * @param queryFileNames
   * @param parallelDegree
   */
  static void e2eNoStartCalciteServer(const string &schemaName,
                                      const vector<string> &queryFileNames,
                                      int parallelDegree);

};

}

#endif //NORMAL_NORMAL_FRONTEND_TEST_TESTUTIL_H
