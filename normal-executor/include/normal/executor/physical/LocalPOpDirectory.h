//
// Created by matt on 25/3/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_LOCALPOPDIRECTORY_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_LOCALPOPDIRECTORY_H

#include <normal/executor/physical/LocalPOpDirectoryEntry.h>
#include <tl/expected.hpp>
#include <unordered_map>
#include <string>

namespace normal::executor::physical {

/**
 * A directory that physical operators use to store information about other physical operators
 */
class LocalPOpDirectory {

public:
  LocalPOpDirectory() = default;
  LocalPOpDirectory(const LocalPOpDirectory&) = default;
  LocalPOpDirectory& operator=(const LocalPOpDirectory&) = default;
  
  void insert(const LocalPOpDirectoryEntry &entry);

  void setComplete(const std::string &name);
  bool allComplete(const POpRelationshipType &relationshipType) const;
  void setIncomplete();

  tl::expected<LocalPOpDirectoryEntry, std::string> get(const std::string& operatorId);
  std::vector<LocalPOpDirectoryEntry> get(const POpRelationshipType &relationshipType);

  std::string showString() const;
  void destroyActorHandles();

private:
  std::unordered_map <std::string, LocalPOpDirectoryEntry> entries_;

  int numProducers = 0;
  int numConsumers = 0;
  int numProducersComplete = 0;
  int numConsumersComplete = 0;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LocalPOpDirectory& directory) {
    return f.object(directory).fields(f.field("entries", directory.entries_),
                                      f.field("numProducers", directory.numProducers),
                                      f.field("numConsumers", directory.numConsumers),
                                      f.field("numProducersComplete", directory.numProducersComplete),
                                      f.field("numConsumersComplete", directory.numConsumersComplete));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_LOCALPOPDIRECTORY_H
