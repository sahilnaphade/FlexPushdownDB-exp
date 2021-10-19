package com.flexpushdowndb.calcite.server;

import com.thrift.calciteserver.CalciteServer;
import com.thrift.calciteserver.InvalidParseRequest;
import com.thrift.calciteserver.TPlanResult;
import org.apache.thrift.TException;

public class CalciteServerHandler implements CalciteServer.Iface{
  public void ping() throws TException {}

  public void shutdown() throws TException{}

  public TPlanResult sql2Plan(String user, String passwd, String catalog, String sql_text, boolean legacySyntax,
                              boolean isexplain) throws InvalidParseRequest, TException{
    return new TPlanResult();
  }

  public void updateMetadata(String catalog, String table) throws TException{}

  public static void main(String[] args) throws Exception {
    CalciteServerHandler handler = new CalciteServerHandler();
    TPlanResult tPlanResult = handler.sql2Plan("", "", "", "", true, true);
    System.out.println("hello");
  }
}
