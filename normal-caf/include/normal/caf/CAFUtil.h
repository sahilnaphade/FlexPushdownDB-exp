//
// Created by Yifei Yang on 1/12/22.
//

#ifndef NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_CAFUTIL_H
#define NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_CAFUTIL_H

#include <caf/all.hpp>

namespace normal::caf {

class CAFUtil {

public:
  inline static constexpr ::caf::type_id_t SegmentCacheActor_first_custom_type_id = ::caf::first_custom_type_id;
  inline static constexpr ::caf::type_id_t Envelope_first_custom_type_id = ::caf::first_custom_type_id + 100;
  inline static constexpr ::caf::type_id_t POpActor_first_custom_type_id = ::caf::first_custom_type_id + 200;
  inline static constexpr ::caf::type_id_t POpActor2_first_custom_type_id = ::caf::first_custom_type_id + 300;
  inline static constexpr ::caf::type_id_t CollatePOp2_first_custom_type_id = ::caf::first_custom_type_id + 400;
  inline static constexpr ::caf::type_id_t FileScanPOp2_first_custom_type_id = ::caf::first_custom_type_id + 500;

};

}


#endif //NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_CAFUTIL_H
