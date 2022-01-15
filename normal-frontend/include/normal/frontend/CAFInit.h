//
// Created by Yifei Yang on 1/14/22.
//

#ifndef NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_CAFINIT_H
#define NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_CAFINIT_H

#include <caf/all.hpp>
#include <caf/io/all.hpp>

namespace normal::frontend {

class CAFInit {

public:
  /**
   * Required before creating actor_system
   */
  static void initCAFGlobalMetaObjects();

};

}


#endif //NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_CAFINIT_H
