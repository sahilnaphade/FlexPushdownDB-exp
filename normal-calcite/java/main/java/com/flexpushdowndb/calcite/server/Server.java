package com.flexpushdowndb.calcite.server;

import com.thrift.calciteserver.CalciteServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

public class Server {
  private final static int SERVER_PORT = 8099;

  public void start() {
    try {
      CalciteServerHandler handler = new CalciteServerHandler();
      CalciteServer.Processor<CalciteServerHandler> processor = new CalciteServer.Processor<>(handler);
      TServerTransport serverTransport = new TServerSocket(SERVER_PORT);
      TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));
      handler.setServerTransport(serverTransport);
      handler.setServer(server);
      System.out.println("Calcite server start...");
      server.serve();
    } catch (Exception e) {
      System.out.println("Calcite server start error");
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    Server server = new Server();
    server.start();
  }
}
