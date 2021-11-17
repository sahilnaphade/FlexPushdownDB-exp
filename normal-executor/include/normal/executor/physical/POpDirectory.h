//
// Created by matt on 24/3/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPDIRECTORY_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPDIRECTORY_H

#include <normal/executor/physical/POpDirectoryEntry.h>
#include <tl/expected.hpp>
#include <string>
#include <unordered_map>

namespace normal::executor::physical {

/**
 * Class for tracking physical operators from outside the actor system.
 */
class POpDirectory {
  using MapType = std::unordered_map<std::string, POpDirectoryEntry>;

private:
  MapType entries_;
  int numOperators_ = 0;
  int numOperatorsComplete_ = 0;

public:
  void insert(const POpDirectoryEntry& entry);
  tl::expected<POpDirectoryEntry, std::string> get(const std::string& name);

  void setComplete(const std::string& name);
  void setIncomplete();
  [[nodiscard]] bool allComplete() const;

  [[nodiscard]] std::string showString() const;
  void clear();

  MapType::iterator begin();
  [[nodiscard]] MapType::const_iterator begin() const;
  MapType::iterator end();
  [[nodiscard]] MapType::const_iterator end() const;

  [[maybe_unused]] [[nodiscard]] MapType::const_iterator cbegin() const;
  [[maybe_unused]] [[nodiscard]] MapType::const_iterator cend() const;

};

}
#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPDIRECTORY_H
