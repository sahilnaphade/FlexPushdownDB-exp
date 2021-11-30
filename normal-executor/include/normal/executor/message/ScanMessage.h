//
// Created by matt on 21/7/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_SCANMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_SCANMESSAGE_H

#include <normal/executor/message/Message.h>
#include <string>
#include <vector>

namespace normal::executor::message {

/**
 * A message to a scan operator telling it to scan a subset of columns from its data source
 */
class ScanMessage : public Message {

public:
  ScanMessage(std::vector<std::string> columnNames_, const std::string &sender, bool resultNeeded);
  [[nodiscard]] const std::vector<std::string> &getColumnNames() const;
  [[nodiscard]] bool isResultNeeded() const;

private:
  std::vector<std::string> columnNames_;
  bool resultNeeded_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_SCANMESSAGE_H
