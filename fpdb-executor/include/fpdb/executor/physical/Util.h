//
// Created by Yifei Yang on 2/22/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_UTIL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_UTIL_H

#include <fpdb/executor/physical/PhysicalOp.h>

namespace fpdb::executor::physical {

class Util {

public:
  /**
   * Connect producers and consumers
   * @param producers
   * @param consumers
   */
  static void connectOneToOne(vector<shared_ptr<PhysicalOp>> &producers,
                              vector<shared_ptr<PhysicalOp>> &consumers);
  static void connectManyToMany(vector<shared_ptr<PhysicalOp>> &producers,
                                vector<shared_ptr<PhysicalOp>> &consumers);
  static void connectManyToOne(vector<shared_ptr<PhysicalOp>> &producers,
                               shared_ptr<PhysicalOp> &consumer);

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_UTIL_H
