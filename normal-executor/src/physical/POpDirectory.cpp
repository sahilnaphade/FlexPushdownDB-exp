//
// Created by matt on 24/3/20.
//

#include <normal/executor/physical/POpDirectory.h>
#include <fmt/format.h>
#include <sstream>

namespace normal::executor::physical {

void POpDirectory::insert(const POpDirectoryEntry& entry) {
  auto inserted = entries_.emplace(entry.getDef()->name(), entry);
  if(!inserted.second)
    throw std::runtime_error(fmt::format("Operator '{}' already added to directory", entry.getDef()->name()));
  ++numOperators_;
}

void POpDirectory::setComplete(const std::string& name) {
  auto entry = entries_.find(name);
  if(entry == entries_.end())
    throw std::runtime_error("No entry for operator '" + name + "'");
  else {
    if (entry->second.isComplete()) {
      throw std::runtime_error("Opdir: Entry for operator '" + name + "'" + "completes twice");
    }
    entry->second.setComplete(true);
  }
  ++numOperatorsComplete_;
}

bool POpDirectory::allComplete() const {
  return numOperatorsComplete_ >= numOperators_;
}

std::string POpDirectory::showString() const {
  std::stringstream ss;
  for(const auto& entry : entries_){
    ss << entry.second.getDef().get() << ": " << entry.second.isComplete() << std::endl;
  }
  return ss.str();
}

void POpDirectory::setIncomplete() {
  for(auto& entry : entries_){
    entry.second.setComplete(false);
  }
}

tl::expected<POpDirectoryEntry, std::string> POpDirectory::get(const std::string& name) {
  auto entryIt = entries_.find(name);
  if(entryIt == entries_.end()){
	return tl::unexpected(fmt::format("Operator with name '{}' not found", name));
  }
  else{
	return entryIt->second;
  }
}

void POpDirectory::clear() {
  entries_.clear();
}

POpDirectory::MapType::iterator POpDirectory::begin() {
  return entries_.begin();
}

POpDirectory::MapType::const_iterator POpDirectory::begin() const {
  return entries_.begin();
}

POpDirectory::MapType::iterator POpDirectory::end() {
  return entries_.end();
}

POpDirectory::MapType::const_iterator POpDirectory::end() const {
  return entries_.end();
}

[[maybe_unused]] POpDirectory::MapType::const_iterator POpDirectory::cbegin() const {
  return entries_.cbegin();
}

  [[maybe_unused]] [[maybe_unused]] POpDirectory::MapType::const_iterator POpDirectory::cend() const {
  return entries_.cend();
}

}