//
// Created by Yifei Yang on 10/13/21.
//

#include <normal/calcite/Calcite.h>
#include <../gen-cpp/CalciteServer.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <string>
#include <iostream>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

int main(int argc, char **argv) {
  std::shared_ptr<TSocket> socket(new TSocket("localhost", 8099));
  std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

  transport->open();

  CalciteServerClient client(protocol);
  TPlanResult result;
  client.sql2Plan(result, "SELECT COUNT(*) FROM LINEORDER", "ssb-sf1-sortlineorder/csv");
  std::cout << result.plan_result << std::endl;
  std::cout << result.execution_time_ms << "ms" << std::endl;
  client.shutdown();

  return 0;
}