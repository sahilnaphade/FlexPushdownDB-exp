//
// Created by Yifei Yang on 1/14/22.
//

#include <normal/frontend/Server.h>

using namespace normal::frontend;

std::shared_ptr<Server> server;

int main() {
  // start CAF server actor system
  server = std::make_shared<Server>();
  server->start();

  // handle exit signals
  auto exitAct = [](int) {
    server->stop();
    server.reset();
    exit(0);
  };
  signal(SIGTERM, exitAct);
  signal(SIGINT, exitAct);
  signal(SIGABRT, exitAct);

  // wait to stop
  std::cout << "Press <Enter> to shutdown the server" << std::endl;
  std::string line;
  std::getline(std::cin, line);
  server->stop();
  server.reset();
  return 0;
}
