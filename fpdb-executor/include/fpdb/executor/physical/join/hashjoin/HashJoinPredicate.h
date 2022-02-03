//
// Created by matt on 29/4/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_JOINPREDICATE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_JOINPREDICATE_H

#include <string>
#include <vector>

using namespace std;

namespace fpdb::executor::physical::join {

/**
 * A join predicate for straight column to column joins
 *
 * TODO: Support expressions
 */
class HashJoinPredicate {
  
public:
  HashJoinPredicate(vector<string> leftColumnNames,
                    vector<string> rightColumnNames);
  HashJoinPredicate() = default;
  HashJoinPredicate(const HashJoinPredicate&) = default;
  HashJoinPredicate& operator=(const HashJoinPredicate&) = default;

  const vector<string> &getLeftColumnNames() const;
  const vector<string> &getRightColumnNames() const;

  string toString() const;

private:
  vector<string> leftColumnNames_;
  vector<string> rightColumnNames_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashJoinPredicate& pred) {
    return f.object(pred).fields(f.field("leftColumnNames", pred.leftColumnNames_),
                                 f.field("rightColumnNames", pred.rightColumnNames_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_JOINPREDICATE_H
