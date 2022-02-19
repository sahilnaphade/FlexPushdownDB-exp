//
// Created by Yifei Yang on 2/15/22.
//

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <spdlog/spdlog.h>
#include <future>
#include "iostream"

using namespace ::arrow::flight;
using namespace ::arrow;

class FlightExceptionServer : public FlightServerBase {

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    SPDLOG_INFO(request.ticket);

    auto schema = ::arrow::schema({
      {field("f0", ::arrow::int32())},
      {field("f1", ::arrow::int32())},
      {field("f2", ::arrow::int32())},
    });

    auto array_0_0 = makeTestArray({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    auto array_0_1 = makeTestArray({10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
    auto array_0_2 = makeTestArray({20, 21, 22, 23, 24, 25, 26, 27, 28, 29});
    auto array_1_0 = makeTestArray({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    auto array_1_1 = makeTestArray({10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
    auto array_1_2 = makeTestArray({20, 21, 22, 23, 24, 25, 26, 27, 28, 29});

    auto rb_0 = ::arrow::RecordBatch::Make(schema, 10, {array_0_0, array_0_1, array_0_2});
    auto rb_1 = ::arrow::RecordBatch::Make(schema, 10, {array_1_0, array_1_1, array_1_2});

    auto rb_reader = ::arrow::RecordBatchReader::Make({rb_0, rb_1});

    auto stream = std::make_unique<RecordBatchStream>(*rb_reader);
    *data_stream = std::move(stream);

    return Status::OK();
  }

  std::shared_ptr<::arrow::Array> makeTestArray(const std::vector<int> &values) {
    std::shared_ptr<::arrow::Array> array;
    std::unique_ptr<::arrow::ArrayBuilder> builder_ptr;
    ::arrow::MakeBuilder(::arrow::default_memory_pool(), ::arrow::int32(), &builder_ptr);
    auto &int32Builder = dynamic_cast<::arrow::Int32Builder &>(*builder_ptr);
    int32Builder.AppendValues(values);
    int32Builder.Finish(&array);
    return array;
  }

};

//int main() {
//  int port = 4321;
//
//  // server
//  Location serverLocation;
//  Location::ForGrpcTcp("0.0.0.0", port, &serverLocation);
//  FlightServerOptions serverOptions(serverLocation);
//
//  auto server = std::make_unique<FlightExceptionServer>();
//  server->Init(serverOptions);
//  server->SetShutdownOnSignals({SIGTERM});
//
//  SPDLOG_INFO("Server listening on localhost:{}", server->port());
//  auto serverFuture = std::async(std::launch::async, [&]() { return server->Serve(); });
//
//  // client
//  Location clientLocation;
//  Location::ForGrpcTcp("localhost", port, &clientLocation);
//  FlightClientOptions clientOptions = FlightClientOptions::Defaults();
//
//  std::unique_ptr<FlightClient> client;
//  auto st = FlightClient::Connect(clientLocation, clientOptions, &client);
//
//  Ticket ticket;
//  ticket.ticket = "DUMMY";
//  std::unique_ptr<::arrow::flight::FlightStreamReader> reader;
//  client->DoGet(ticket, &reader);
//
//  std::shared_ptr<::arrow::Table> table;
//  st = reader->ReadAll(&table);
//
//  auto printOptions = ::arrow::PrettyPrintOptions::Defaults();
//  printOptions.skip_new_lines = true;
//  st = ::arrow::PrettyPrint(*table, printOptions, &std::cout);
//
//  // stop
//  server->Shutdown();
//  server->Wait();
//  serverFuture.wait();
//
//  return 0;
//}