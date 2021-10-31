package com.flexpushdowndb.calcite.server;

import com.thrift.calciteserver.CalciteServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.ini4j.Ini;

import java.io.IOException;
import java.io.InputStream;

public class Server {
  private final int SERVER_PORT;

  public Server() throws IOException {
    InputStream is = getClass().getResourceAsStream("/config/exec.conf");
    Ini ini = new Ini(is);
    this.SERVER_PORT = Integer.parseInt(ini.get("conf", "SERVER_PORT"));
  }

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

  public static void main(String[] args) throws IOException {
    Server server = new Server();
    server.start();
  }
}
