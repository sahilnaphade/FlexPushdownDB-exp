//
// Created by Yifei Yang on 1/14/22.
//

#include <normal/frontend/CAFInit.h>
#include <normal/executor/physical/POpActor.h>
#include <normal/executor/physical/POpActor2.h>
#include <normal/executor/physical/collate/CollatePOp2.h>
#include <normal/executor/physical/file/FileScanPOp2.h>
#include <normal/executor/cache/SegmentCacheActor.h>
#include <normal/executor/message/Envelope.h>
#include <normal/executor/serialization/MessageSerializer.h>

namespace normal::frontend {

void CAFInit::initCAFGlobalMetaObjects() {
  ::caf::init_global_meta_objects<::caf::id_block::SegmentCacheActor>();
  ::caf::init_global_meta_objects<::caf::id_block::Envelope>();
  ::caf::init_global_meta_objects<::caf::id_block::POpActor>();
  ::caf::init_global_meta_objects<::caf::id_block::POpActor2>();
  ::caf::init_global_meta_objects<::caf::id_block::CollatePOp2>();
  ::caf::init_global_meta_objects<::caf::id_block::FileScanPOp2>();
  ::caf::init_global_meta_objects<::caf::id_block::TupleSet>();
  ::caf::init_global_meta_objects<::caf::id_block::Message>();
  ::caf::init_global_meta_objects<::caf::id_block::SegmentKey>();
  ::caf::init_global_meta_objects<::caf::id_block::Partition>();
  ::caf::init_global_meta_objects<::caf::id_block::SegmentMetadata>();
  ::caf::init_global_meta_objects<::caf::id_block::SegmentData>();
  ::caf::init_global_meta_objects<::caf::id_block::Column>();
  ::caf::init_global_meta_objects<::caf::id_block::TupleSetIndex>();
  ::caf::init_global_meta_objects<::caf::id_block::TupleKey>();
  ::caf::init_global_meta_objects<::caf::id_block::TupleKeyElement>();

  ::caf::core::init_global_meta_objects();
  ::caf::io::middleman::init_global_meta_objects();
}

}
