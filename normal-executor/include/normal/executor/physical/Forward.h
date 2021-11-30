//
// Created by matt on 15/9/20.
//
// Forward declarations

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FORWARD_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FORWARD_H

namespace normal::executor {

namespace physical {
//  class OperatorManager;
  class PhysicalOp;
  class POpActor;
  class POpContext;

  namespace collate {
    class CollateState;
  }

  namespace file {
    class FileScanState;
  }

  namespace s3 {
    class [[maybe_unused]] S3SelectScanState;
  }
}

namespace cache {
  class SegmentCacheActor;
}

//namespace graph {
//  class OperatorGraph;
//}

namespace message {
  class Envelope;
}

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FORWARD_H
