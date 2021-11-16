//
// Created by matt on 25/3/20.
//

#include <normal/executor/physical/LocalPOpDirectory.h>
#include <spdlog/spdlog.h>
#include <sstream>

namespace normal::executor::physical {

void LocalPOpDirectory::insert(const LocalPOpDirectoryEntry& entry) {
  // map insert cannot cover the value for the same key, need to delete first
  auto iter = entries_.find(entry.name());
  if (iter != entries_.end()) {
    entries_.erase(iter);
  }
  entries_.emplace(entry.name(), entry);

  switch (entry.relationshipType()) {
    case POpRelationshipType::Producer: ++numProducers; break;
    case POpRelationshipType::Consumer: ++numConsumers; break;
    case POpRelationshipType::None: throw std::runtime_error("Unconnected operator not supported");
  }
}

void LocalPOpDirectory::setComplete(const std::string& name) {
  auto entry = entries_.find(name);
  if(entry == entries_.end())
    throw std::runtime_error("No entry for actor '" + name + "'");
  else {
  if (entry->second.complete()) {
    throw std::runtime_error("LocalOpdir: Entry for operator '" + name + "'" + "completes twice");
  }
	entry->second.complete(true);
	switch (entry->second.relationshipType()) {
    case POpRelationshipType::Producer: ++numProducersComplete; break;
    case POpRelationshipType::Consumer: ++numConsumersComplete; break;
    case POpRelationshipType::None:throw std::runtime_error("Unconnected operator not supported");
	}
  }
}

std::string LocalPOpDirectory::showString() const {
  std::stringstream ss;
  for(const auto& entry : entries_){
    ss << "{name: " << entry.second.name() << ", complete: " << entry.second.complete() << "}" << std::endl;
  }
  return ss.str();
}

void LocalPOpDirectory::setIncomplete() {
  for(auto& entry : entries_){
    entry.second.complete(false);
  }
  numProducersComplete = 0;
  numConsumersComplete = 0;
}

bool LocalPOpDirectory::allComplete(const POpRelationshipType &operatorRelationshipType) const {
  switch (operatorRelationshipType) {
    case POpRelationshipType::Producer: return numProducersComplete >= numProducers;
    case POpRelationshipType::Consumer: return numConsumersComplete >= numConsumers;
    case POpRelationshipType::None:
      throw std::runtime_error("Unconnected operator not supported");
  }
}

void LocalPOpDirectory::destroyActorHandles(){
  for(auto &entry: entries_){
	entry.second.destroyActor();
  }
}

tl::expected<LocalPOpDirectoryEntry, std::string> LocalPOpDirectory::get(const std::string &operatorId) {
  auto entryIt = entries_.find(operatorId);
  if(entryIt == entries_.end()){
    auto message = fmt::format("Operator with id '{}' not found", operatorId);
    SPDLOG_DEBUG(message);
	SPDLOG_DEBUG("Operator directory:\n{}", showString());
	return tl::unexpected(message);
  }
  else{
	return entryIt->second;
  }
}

std::vector<LocalPOpDirectoryEntry> LocalPOpDirectory::get(const POpRelationshipType &relationshipType) {
  std::vector<LocalPOpDirectoryEntry> matchingEntries;
  for(const auto& operatorEntry: entries_){
    if(operatorEntry.second.relationshipType() == relationshipType){
	  matchingEntries.emplace_back(operatorEntry.second);
    }
  }
  return matchingEntries;
}

}