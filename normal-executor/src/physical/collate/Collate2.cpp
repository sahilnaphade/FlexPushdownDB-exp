//
// Created by matt on 9/10/20.
//

#include <normal/executor/physical/collate/Collate2.h>

namespace normal::executor::physical::collate {

[[maybe_unused]] CollateActor::behavior_type CollateFunctor(CollateStatefulActor self,
                     const std::string &name,
                     long queryId,
                     const caf::actor &rootActorHandle,
                     const caf::actor &segmentCacheActorHandle) {

  self->state.setState(self,
					   name,
					   queryId,
					   rootActorHandle,
					   segmentCacheActorHandle);

  return self->state.makeBehavior(self);
}

}