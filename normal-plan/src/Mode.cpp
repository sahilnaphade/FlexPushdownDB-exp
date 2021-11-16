//
// Created by Yifei Yang on 7/27/20.
//

#include <normal/plan/Mode.h>
#include <string>

namespace normal::plan {

Mode::Mode(ModeId id): id_(id) {}

ModeId Mode::id() const {
  return id_;
}

std::string Mode::toString() {
  switch(id_) {
    case Pullup: return "Pullup";
    case Pushdown: return "Pushdown";
    case CachingOnly: return "Caching-only";
    case Hybrid: return "Hybrid";
    default:
      /*
       * Shouldn't occur, but we'll throw a serious-ish exception if it ever does
       */
      throw std::domain_error("Cannot get string for mode '" + std::to_string(id_ )+ "'. Unrecognized mode");
  }
}

bool Mode::is(const std::shared_ptr<Mode>& mode) {
  return id_ == mode->id_;
}

std::shared_ptr<Mode> Mode::pullupMode() {
  return std::make_shared<Mode>(Pullup);
}

std::shared_ptr<Mode> Mode::pushdownMode() {
  return std::make_shared<Mode>(Pushdown);
}

std::shared_ptr<Mode> Mode::cachingOnlyMode() {
  return std::make_shared<Mode>(CachingOnly);
}

std::shared_ptr<Mode> Mode::hybridMode() {
  return std::make_shared<Mode>(Hybrid);
}

}
