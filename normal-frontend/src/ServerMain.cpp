//
// Created by Yifei Yang on 1/14/22.
//

#include <normal/frontend/Server.h>
#include <signal.h>

using namespace normal::frontend;

std::shared_ptr<Server> server;

int main() {
  // handle exit signals
  auto exitAct = [](int) {
    server->stop();
    server.reset();
    exit(0);
  };
  signal(SIGTERM, exitAct);
  signal(SIGINT, exitAct);
  signal(SIGABRT, exitAct);

  // start CAF server actor system
  server = std::make_shared<Server>();
  server->start();

  // wait to stop
  std::cout << "Press <Enter> to shutdown the server" << std::endl;
  getchar();
  server->stop();
  server.reset();
  return 0;
}
