//
// Created by Yifei Yang on 2/9/21.
//

#include <normal/util/Util.h>
#include <fmt/format.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <arpa/inet.h>
#include <ifaddrs.h>

using namespace normal::util;

string normal::util::readFile(const string& filePath) {
  ifstream inFile(filePath);
  if (!inFile.good()) {
    throw runtime_error(fmt::format("File not exists: {}", filePath));
  }
  ostringstream buf;
  char ch;
  while (buf && inFile.get(ch)) {
    buf.put(ch);
  }
  return buf.str();
}

vector<string> normal::util::readFileByLine(const string &filePath) {
  ifstream inFile(filePath);
  if (!inFile.good()) {
    throw runtime_error(fmt::format("File not exists: {}", filePath));
  }
  vector<string> lines;
  string line;
  while (getline(inFile, line)) {
    lines.emplace_back(line);
  }
  return lines;
}

unordered_map<string, string> normal::util::readConfig(const string &fileName) {
  unordered_map<string, string> configMap;
  string configPath = filesystem::current_path()
          .parent_path()
          .append("resources/config")
          .append(fileName)
          .string();
  vector<string> lines = readFileByLine(configPath);
  for(auto const &line: lines) {
    auto pos = line.find('=');
    if (pos == string::npos) {
      continue;
    }
    auto key = line.substr(0, pos);
    auto value = line.substr(pos + 1);
    configMap[key] = value;
  }
  return configMap;
}

vector<string> normal::util::readRemoteIps() {
  auto expLocalIp = getLocalIp();
  if (!expLocalIp) {
    throw runtime_error(expLocalIp.error());
  }
  const auto &localIp = *expLocalIp;
  string clusterIpFilePath = filesystem::current_path()
          .parent_path()
          .append("resources/config/cluster_ips")
          .string();
  auto clusterIps = readFileByLine(clusterIpFilePath);
  set<string> clusterIpSet(clusterIps.begin(), clusterIps.end());
  clusterIpSet.erase(localIp);
  return vector<string>(clusterIpSet.begin(), clusterIpSet.end());
}

bool normal::util::parseBool(const string& stringToParse) {
  if (stringToParse == "TRUE") {
    return true;
  } else {
    return false;
  }
}

size_t normal::util::hashCombine(const vector<size_t> &hashes) {
  size_t hash = 17;
  for (const auto &singleHash: hashes) {
    hash = hash * 31 + singleHash;
  }
  return hash;
}

bool normal::util::isInteger(const string& str) {
  try {
    int parsedInt = stoi(str);
  } catch (const logic_error& err) {
    return false;
  }
  return true;
}

tl::expected<string, string> normal::util::getLocalIp() {
  string ipv4Addr;
  struct ifaddrs *ifAddrStruct = nullptr;
  struct ifaddrs *ifa;
  void *tmpAddrPtr;

  getifaddrs(&ifAddrStruct);

  for (ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next) {
    if (!ifa->ifa_addr) {
      continue;
    }
    if (ifa->ifa_addr->sa_family == AF_INET) { // check it is IPv4
      // is a valid IP4 Address
      tmpAddrPtr=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      if (strcmp(ifa->ifa_name, "lo0") == 0) {
        // this is loopback address
        continue;
      }
      ipv4Addr = string(addressBuffer);
      break;
    }
  }
  if (ifAddrStruct != nullptr) freeifaddrs(ifAddrStruct);

  if (ipv4Addr.empty()) {
    return tl::make_unexpected("Cannot find local IP address");
  } else {
    return ipv4Addr;
  }
}
