//
// Created by matt on 29/4/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_JOINPREDICATE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_JOINPREDICATE_H

#include <string>
#include <vector>

using namespace std;

namespace normal::executor::physical::hashjoin {

/**
 * A join predicate for straight column to column joins
 *
 * TODO: Support expressions
 */
class HashJoinPredicate {
  
public:
  HashJoinPredicate(vector<string> leftColumnNames,
                    vector<string> rightColumnNames);

  const vector<string> &getLeftColumnNames() const;
  const vector<string> &getRightColumnNames() const;

  string toString() const;

private:
  vector<string> leftColumnNames_;
  vector<string> rightColumnNames_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_JOINPREDICATE_H
