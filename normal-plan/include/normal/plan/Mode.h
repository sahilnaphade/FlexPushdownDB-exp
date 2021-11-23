//
// Created by Yifei Yang on 7/27/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_MODE_MODE_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_MODE_MODE_H

#include <memory>

namespace normal::plan {

enum ModeId {
  PULL_UP,
  PUSHDOWN_ONLY,
  CACHING_ONLY,
  HYBRID
};

class Mode {
public:
  explicit Mode(ModeId modeId);
  ~Mode() = default;

  ModeId id() const;
  bool is(const std::shared_ptr<Mode>& mode);
  std::string toString();

  static std::shared_ptr<Mode> pullupMode();
  static std::shared_ptr<Mode> pushdownOnlyMode();
  static std::shared_ptr<Mode> cachingOnlyMode();
  static std::shared_ptr<Mode> hybridMode();

private:
  ModeId id_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_MODE_MODE_H
