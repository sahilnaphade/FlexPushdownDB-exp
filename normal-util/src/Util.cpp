//
// Created by Yifei Yang on 2/9/21.
//

#include <normal/util/Util.h>
#include <fmt/format.h>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <iostream>
#include <vector>

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

bool normal::util::parseBool(const string& stringToParse) {
  if (stringToParse == "TRUE") {
    return true;
  } else {
    return false;
  }
}

bool normal::util::isInteger(const string& str) {
  try {
    int parsedInt = stoi(str);
  } catch (const logic_error& err) {
    return false;
  }
  return true;
}

string normal::util::getLocalIp() {
  char hostBuffer[256];
  int hostName = gethostname(hostBuffer, sizeof(hostBuffer));
  struct hostent *host_entry = gethostbyname(hostBuffer);
  if (host_entry == nullptr) {
    cerr << "Cannot get local ip" << endl;
    return "";
  }
  char *IPBuffer = inet_ntoa(*((struct in_addr*)host_entry->h_addr_list[0]));
  return string(IPBuffer);
}
