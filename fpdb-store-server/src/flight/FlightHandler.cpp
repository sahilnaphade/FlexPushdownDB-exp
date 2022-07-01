//
// Created by matt on 4/2/22.
//

#include "fpdb/store/server/flight/FlightHandler.hpp"
#include "fpdb/store/server/flight/HeaderMiddlewareFactory.hpp"
#include "fpdb/store/server/flight/GetObjectTicket.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fpdb/executor/physical/serialization/PhysicalPlanDeserializer.h"
#include "fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h"
#include "fpdb/executor/physical/filter/FilterPOp.h"
#include "fpdb/executor/caf/CAFInit.h"
#include "fpdb/executor/FPDBStoreExecution.h"
#include "fpdb/tuple/serialization/ArrowSerializer.h"
#include "fpdb/tuple/arrow/Arrays.h"
#include "fpdb/tuple/util/Util.h"

using namespace fpdb::executor::physical;
using namespace fpdb::tuple;

namespace fpdb::store::server::flight {

FlightHandler::FlightHandler(Location location,
                             std::string store_root_path,
                             std::shared_ptr<::caf::actor_system> actor_system) :
  location_(std::move(location)),
  store_root_path_(std::move(store_root_path)),
  actor_system_(std::move(actor_system)) {}

FlightHandler::~FlightHandler() {
  this->shutdown();
}

tl::expected<void, std::string> FlightHandler::init() {
  FlightServerOptions options(location_);
  options.middleware.emplace_back(HeaderMiddlewareKey, std::make_shared<HeaderMiddlewareFactory>());
  auto st = this->Init(options);
  if(!st.ok()) {
    return tl::make_unexpected(st.message());
  }

  // init vars for bitmap caches
  init_bitmap_cache();

  // NOTE: This appears to swallow the signal and not allow anything else to handle it
  //  this->SetShutdownOnSignals({SIGTERM});

  // Init CAF objects for executor
  fpdb::executor::caf::CAFInit::initCAFGlobalMetaObjects();

  return {};
}

tl::expected<void, std::string> FlightHandler::serve() {
  auto st = this->Serve();
  if(!st.ok()) {
    return tl::make_unexpected(st.message());
  }
  return {};
}

tl::expected<void, std::string> FlightHandler::shutdown() {
  auto st = this->Shutdown();
  if(!st.ok()) {
    return tl::make_unexpected(st.message());
  }
  return {};
}

tl::expected<void, std::string> FlightHandler::wait() {
  auto st = this->Wait();
  if(!st.ok()) {
    return tl::make_unexpected(st.message());
  }
  return {};
}

int FlightHandler::port() {
  return FlightServerBase::port();
}

::arrow::Status FlightHandler::GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                                             std::unique_ptr<FlightInfo>* info) {
  auto expected_flight_info = get_flight_info(context, request);
  if(!expected_flight_info.has_value()) {
    return expected_flight_info.error();
  }
  *info = std::move(expected_flight_info.value());
  return ::arrow::Status::OK();
}

::arrow::Status FlightHandler::DoGet(const ServerCallContext& context, const Ticket& request,
                                     std::unique_ptr<FlightDataStream>* stream) {
  auto expected_flight_stream = do_get(context, request);
  if(!expected_flight_stream.has_value()) {
    return expected_flight_stream.error();
  }
  *stream = std::move(expected_flight_stream.value());
  return ::arrow::Status::OK();
}

::arrow::Status FlightHandler::DoPut(const ServerCallContext& context,
                                     std::unique_ptr<FlightMessageReader> reader,
                                     std::unique_ptr<FlightMetadataWriter>) {
  auto put_result = do_put(context, reader);
  if (!put_result.has_value()) {
    return put_result.error();
  }
  return ::arrow::Status::OK();
}

tl::expected<HeaderMiddleware*, std::string>
FlightHandler::get_header_middleware(const ::arrow::flight::ServerCallContext& context) {
  auto* header_middleware = context.GetMiddleware(std::string{HeaderMiddlewareKey.data(), HeaderMiddlewareKey.size()});
  if(header_middleware == nullptr) {
    return tl::make_unexpected("HeaderMiddleware not found in ServerCallContext");
  }
  return dynamic_cast<HeaderMiddleware*>(header_middleware);
}

tl::expected<std::string, std::string> FlightHandler::parse_header(const std::string& key,
                                                                   const HeaderMiddleware& middleware) {
  auto it = middleware.headers().find(key);
  if(it == middleware.headers().end()) {
    return tl::make_unexpected(fmt::format("Header '{}' not found", key));
  }
  return std::string{it->second};
}

tl::expected<std::unique_ptr<FlightInfo>, ::arrow::Status>
FlightHandler::get_flight_info(const ServerCallContext& context, const FlightDescriptor& request) {

  auto expected_middleware = get_header_middleware(context);
  if(!expected_middleware.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Internal, "Internal error"));
  }
  auto* middleware = expected_middleware.value();

  auto expected_bucket = parse_header({BucketHeaderKey.data(), BucketHeaderKey.size()}, *middleware);
  if(!expected_bucket.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, expected_bucket.error()));
  }
  auto bucket = expected_bucket.value();

  auto expected_object = parse_header({ObjectHeaderKey.data(), ObjectHeaderKey.size()}, *middleware);
  if(!expected_object.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, expected_object.error()));
  }
  auto object = expected_object.value();

  switch(request.type) {
    case FlightDescriptor::UNKNOWN: {
      return tl::make_unexpected(MakeFlightError(
        FlightStatusCode::Failed, fmt::format("FlightDescriptor type '{}' not supported", request.type)));
    }
    case FlightDescriptor::PATH: {
      return get_flight_info_for_path(context, request, bucket, object);
    }
    case FlightDescriptor::CMD: {
      return get_flight_info_for_cmd(context, request, bucket, object);
    }
  }

  return tl::make_unexpected(
    MakeFlightError(FlightStatusCode::Failed, fmt::format("Unrecognized FlightDescriptor type '{}'", request.type)));
}

tl::expected<std::unique_ptr<FlightInfo>, ::arrow::Status>
FlightHandler::get_flight_info_for_path(const ServerCallContext&, const FlightDescriptor& request,
                                        std::string bucket, std::string object) {

  // Create the ticket
  auto ticket_object = GetObjectTicket::make(std::move(bucket), std::move(object));
  auto exp_ticket = ticket_object->to_ticket(false);
  if (!exp_ticket.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, exp_ticket.error()));
  }

  // Create the endpoints (dummy for now)
  std::vector<FlightEndpoint> endpoints;
  FlightEndpoint endpoint{*exp_ticket, {}};
  endpoints.push_back(endpoint);

  // Create the FlightInfo object, contains the original descriptor and the endpoints to find the data
  FlightInfo::Data flight_data;
  flight_data.descriptor = request;
  flight_data.endpoints = endpoints;
  auto schemaBytes = ArrowSerializer::schema_to_bytes(::arrow::schema({}));
  flight_data.schema = std::string(schemaBytes.begin(), schemaBytes.end());
  flight_data.total_records = -1;
  flight_data.total_bytes = -1;

  return std::make_unique<FlightInfo>(flight_data);
}

tl::expected<std::unique_ptr<FlightInfo>, ::arrow::Status>
FlightHandler::get_flight_info_for_cmd(const ServerCallContext& context, const FlightDescriptor& request,
                                       std::string, std::string) {

  // Parse the cmd object
  auto expected_cmd_object = CmdObject::deserialize(request.cmd);
  if(!expected_cmd_object.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, expected_cmd_object.error()));
  }
  auto cmd_object = expected_cmd_object.value();

  switch(cmd_object->type()->id()) {
    case CmdTypeId::SELECT_OBJECT_CONTENT: {
      auto select_object_content_cmd = std::static_pointer_cast<SelectObjectContentCmd>(cmd_object);
      return get_flight_info_for_select_object_content_cmd(context, request, select_object_content_cmd);
    }
    default: {
      return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed,
                                                 fmt::format("Unknown cmd type: '{}'", cmd_object->type()->name())));
    }
  }
}

tl::expected<std::unique_ptr<FlightInfo>, ::arrow::Status> FlightHandler::get_flight_info_for_select_object_content_cmd(
  const ServerCallContext&, const FlightDescriptor& request,
  const std::shared_ptr<SelectObjectContentCmd>& select_object_content_cmd) {

  // Create the ticket
  auto ticket_object = SelectObjectContentTicket::make(0,
                                                       select_object_content_cmd->query_plan_string());
  auto exp_ticket = ticket_object->to_ticket(false);
  if (!exp_ticket.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, exp_ticket.error()));
  }

  // Create the endpoints (dummy for now)
  std::vector<FlightEndpoint> endpoints;
  FlightEndpoint endpoint{*exp_ticket, {}};
  endpoints.push_back(endpoint);

  // Create the FlightInfo object, contains the original descriptor and the endpoints to find the data
  FlightInfo::Data flight_data;
  flight_data.descriptor = request;
  flight_data.endpoints = endpoints;
  auto schemaBytes = ArrowSerializer::schema_to_bytes(::arrow::schema({}));
  flight_data.schema = std::string(schemaBytes.begin(), schemaBytes.end());
  flight_data.total_records = -1;
  flight_data.total_bytes = -1;

  return std::make_unique<FlightInfo>(flight_data);
}

tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status> FlightHandler::do_get(const ServerCallContext& context,
                                                                                       const Ticket& request) {

  auto expected_ticket_object = TicketObject::deserialize(request);
  if(!expected_ticket_object.has_value()) {
    return tl::make_unexpected(
            MakeFlightError(FlightStatusCode::Failed, fmt::format("Cannot parse Flight Ticket '{}'", request.ticket)));
  }
  auto ticket_object = expected_ticket_object.value();

  switch(ticket_object->type()->id()) {
    case TicketTypeId::GET_OBJECT: {
      auto get_object_ticket = std::static_pointer_cast<GetObjectTicket>(ticket_object);
      return do_get_get_object(context, get_object_ticket);
    }
    case TicketTypeId::SELECT_OBJECT_CONTENT: {
      auto select_object_content_ticket = std::static_pointer_cast<SelectObjectContentTicket>(ticket_object);
      return do_get_select_object_content(context, select_object_content_ticket);
    }
    case TicketTypeId::GET_BITMAP: {
      auto get_bitmap_ticket = std::static_pointer_cast<GetBitmapTicket>(ticket_object);
      return do_get_get_bitmap(context, get_bitmap_ticket);
    }
  }

  return tl::make_unexpected(MakeFlightError(
          FlightStatusCode::Failed, fmt::format("Unrecognized Flight Ticket type '{}'", ticket_object->type()->name())));
}

tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status>
FlightHandler::do_get_get_object(const ServerCallContext&,
                                 const std::shared_ptr<GetObjectTicket>&) {
  // TODO: currently just return a toy table
  auto schema = ::arrow::schema({
                                        {field("f0", ::arrow::int32())},
                                        {field("f1", ::arrow::int32())},
                                        {field("f2", ::arrow::int32())},
                                });

  auto array_0_0 = Arrays::make<::arrow::Int32Type>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}).value();
  auto array_0_1 = Arrays::make<::arrow::Int32Type>({10, 11, 12, 13, 14, 15, 16, 17, 18, 19}).value();
  auto array_0_2 = Arrays::make<::arrow::Int32Type>({20, 21, 22, 23, 24, 25, 26, 27, 28, 29}).value();

  auto rb_0 = ::arrow::RecordBatch::Make(schema, 10, {array_0_0, array_0_1, array_0_2});
  auto rb_1 = ::arrow::RecordBatch::Make(schema, 10, {array_0_0, array_0_1, array_0_2});

  auto rb_reader = ::arrow::RecordBatchReader::Make({rb_0, rb_1});

  return std::make_unique<::arrow::flight::RecordBatchStream>(*rb_reader);
}

tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status> FlightHandler::do_get_select_object_content(
  const ServerCallContext&, const std::shared_ptr<SelectObjectContentTicket>& select_object_content_ticket) {

  // query id
  auto query_id = select_object_content_ticket->query_id();

  // deserialize the query plan
  auto exp_physical_plan = PhysicalPlanDeserializer::deserialize(select_object_content_ticket->query_plan_string(),
                                                                 store_root_path_);
  if (!exp_physical_plan.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, exp_physical_plan.error()));
  }
  auto physical_plan = *exp_physical_plan;

  // wait for bitmaps ready if required
  for (const auto &op_it: physical_plan->getPhysicalOps()) {
    auto op = op_it.second;

    // for each BloomFilterUsePOp, wait until its bitmap is ready
    if (op->getType() == POpType::BLOOM_FILTER_USE) {
      auto typedOp = std::static_pointer_cast<bloomfilter::BloomFilterUsePOp>(op);
      auto bloomFilter = typedOp->getBloomFilter();
      if (!bloomFilter.has_value()) {
        return tl::make_unexpected(
                MakeFlightError(FlightStatusCode::Failed,
                                fmt::format("Bloom filter not set in BloomFilterUsePOp: '{}'", op->name())));
      }

      // skip invalid bloom filter
      if (!(*bloomFilter)->valid()) {
        continue;
      }

      // wait for bitmap ready
      auto bitmap_key = BitmapCache::generateKey(query_id, op->name());
      auto bitmap = get_bitmap_from_cache(bitmap_key, BitmapType::BLOOM_FILTER_COMPUTE);
      if (!bitmap.has_value()) {
        return tl::make_unexpected(
                MakeFlightError(FlightStatusCode::Failed,
                                fmt::format("Bloom filter bitmap with key '{}' is not valid", bitmap_key)));
      }
      (*bloomFilter)->setBitArray(*bitmap);
    }

    // for each filter with bitmap pushdown enabled, wait until its bitmap is ready
    else if (op->getType() == POpType::FILTER) {
      auto typedOp = std::static_pointer_cast<filter::FilterPOp>(op);
      if (!typedOp->isBitmapPushdownEnabled()) {
        continue;
      }

      // wait for bitmap ready
      auto bitmap_key = BitmapCache::generateKey(query_id, typedOp->getBitmapWrapper()->mirrorOp_);
      auto bitmap = get_bitmap_from_cache(bitmap_key, BitmapType::FILTER_COMPUTE);
      typedOp->setBitmap(bitmap);
    }
  }

  // execute the query plan
  auto execution = std::make_shared<fpdb::executor::FPDBStoreExecution>(query_id,
                                                                        actor_system_,
                                                                        physical_plan);

  // get query result
  auto table = execution->execute()->table();

  // buffer bitmaps generated at storage side
  for (const auto &bitmap_it: execution->getBitmaps()) {
    auto bitmap_key = BitmapCache::generateKey(query_id, bitmap_it.first);
    put_bitmap_into_cache(bitmap_key, bitmap_it.second, BitmapType::FILTER_STORAGE, true);
  }

  // make record batch stream, need to specially handle when num_rows = 0
  ::arrow::RecordBatchVector batches;

  if (table->num_rows() > 0) {
    std::shared_ptr<arrow::RecordBatch> batch;
    ::arrow::TableBatchReader tbl_reader(*table);
    tbl_reader.set_chunksize(fpdb::tuple::DefaultChunkSize);

    auto status = tbl_reader.ReadAll(&batches);
    if (!status.ok()) {
      return tl::make_unexpected(status);
    }
  } else {
    ::arrow::ArrayVector arrayVec;
    for (const auto &field: table->schema()->fields()) {
      auto expArray = fpdb::tuple::util::Util::makeEmptyArray(field->type());
      if (!expArray.has_value()) {
        return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, expArray.error()));
      }
      arrayVec.emplace_back(*expArray);
    }
    batches.emplace_back(::arrow::RecordBatch::Make(table->schema(), 0, arrayVec));
  }

  auto rb_reader = ::arrow::RecordBatchReader::Make(batches);
  return std::make_unique<::arrow::flight::RecordBatchStream>(*rb_reader);
}

tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status> FlightHandler::do_get_get_bitmap(
        const ServerCallContext&,const std::shared_ptr<GetBitmapTicket>& get_bitmap_ticket) {

  // get bitmap from bitmap cache
  auto query_id = get_bitmap_ticket->query_id();
  auto op = get_bitmap_ticket->op();
  auto bitmap_key = BitmapCache::generateKey(query_id, op);
  auto bitmap = get_bitmap_from_cache(bitmap_key, BitmapType::FILTER_STORAGE);
  if (!bitmap.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed,
                                               fmt::format("Storage bitmap with key '{}' is not valid", bitmap_key)));
  }

  // make recordBatch from bitmap
  auto exp_record_batch = ArrowSerializer::bitmap_to_recordBatch(*bitmap);
  if (!exp_record_batch .has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, exp_record_batch .error()));
  }

  // make flightDataStream
  auto rb_reader = ::arrow::RecordBatchReader::Make({*exp_record_batch });
  return std::make_unique<::arrow::flight::RecordBatchStream>(*rb_reader);
}

tl::expected<void, ::arrow::Status> FlightHandler::do_put(const ServerCallContext& context,
                                                          const std::unique_ptr<FlightMessageReader> &reader) {
  switch(reader->descriptor().type) {
    case FlightDescriptor::CMD: {
      return do_put_for_cmd(context, reader);
    }
    default: {
      return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed,
                                                 fmt::format("FlightDescriptor type '{}' not supported for DoPut",
                                                             reader->descriptor().type)));
    }
  }
}

tl::expected<void, ::arrow::Status>
FlightHandler::do_put_for_cmd(const ServerCallContext& context,
                              const std::unique_ptr<FlightMessageReader> &reader) {
  // Parse the cmd object
  auto expected_cmd_object = CmdObject::deserialize(reader->descriptor().cmd);
  if(!expected_cmd_object.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, expected_cmd_object.error()));
  }
  auto cmd_object = expected_cmd_object.value();

  switch(cmd_object->type()->id()) {
    case CmdTypeId::PUT_BITMAP: {
      auto put_bitmap_cmd = std::static_pointer_cast<PutBitmapCmd>(cmd_object);
      return do_put_put_bitmap(context, put_bitmap_cmd, reader);
    }
    case CmdTypeId::CLEAR_BITMAP: {
      auto clear_bitmap_cmd = std::static_pointer_cast<ClearBitmapCmd>(cmd_object);
      return do_put_clear_bitmap(context, clear_bitmap_cmd);
    }
    default: {
      return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed,
                                                 fmt::format("Cmd type '{}' not supported for DoPut",
                                                             cmd_object->type()->name())));;
    }
  }
}

tl::expected<void, ::arrow::Status>
FlightHandler::do_put_put_bitmap(const ServerCallContext&,
                                 const std::shared_ptr<PutBitmapCmd>& put_bitmap_cmd,
                                 const std::unique_ptr<FlightMessageReader> &reader) {
  // bitmap key
  auto bitmap_key = BitmapCache::generateKey(put_bitmap_cmd->query_id(), put_bitmap_cmd->op());
  auto valid = put_bitmap_cmd->valid();

  // bitmap
  ::arrow::RecordBatchVector recordBatches;
  auto status = reader->ReadAll(&recordBatches);
  if (!status.ok()) {
    return tl::make_unexpected(status);
  }

  if (recordBatches.size() != 1) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed,
                                               "Bitmap recordBatch stream should only contain one recordBatch"));
  }
  auto exp_bitmap = ArrowSerializer::recordBatch_to_bitmap(recordBatches[0]);
  if (!exp_bitmap.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, exp_bitmap.error()));
  }

  // put bitmap
  put_bitmap_into_cache(bitmap_key, *exp_bitmap, put_bitmap_cmd->bitmap_type(), valid);

  return {};
}

tl::expected<void, ::arrow::Status>
FlightHandler::do_put_clear_bitmap(const ServerCallContext&,
                                   const std::shared_ptr<ClearBitmapCmd>& clear_bitmap_cmd) {
  // bitmap key
  auto bitmap_key = BitmapCache::generateKey(clear_bitmap_cmd->query_id(), clear_bitmap_cmd->op());

  // just consume the bitmap
  get_bitmap_from_cache(bitmap_key, clear_bitmap_cmd->bitmap_type());

  return {};
}

std::optional<std::vector<int64_t>> FlightHandler::get_bitmap_from_cache(const std::string &key,
                                                                         BitmapType bitmap_type) {
  // get corresponding vars
  auto& bitmap_cache = bitmap_cache_map[bitmap_type];
  auto& bitmap_mutex = bitmap_mutex_map[bitmap_type];
  auto& bitmap_cvs = bitmap_cvs_map[bitmap_type];

  // get bitmap until it's ready
  std::unique_lock lock(*bitmap_mutex);

  tl::expected<std::optional<std::vector<int64_t>>, std::string> exp_bitmap;
  auto cv = std::make_shared<std::condition_variable_any>();
  bitmap_cvs[key] = cv;

  cv->wait(lock, [&] {
    exp_bitmap = bitmap_cache->consumeBitmap(key);
    return exp_bitmap.has_value();
  });

  bitmap_cvs.erase(key);
  return *exp_bitmap;
}

void FlightHandler::put_bitmap_into_cache(const std::string &key,
                                          const std::vector<int64_t> &bitmap,
                                          BitmapType bitmap_type,
                                          bool valid) {
  // get corresponding vars
  auto& bitmap_cache = bitmap_cache_map[bitmap_type];
  auto& bitmap_mutex = bitmap_mutex_map[bitmap_type];
  auto& bitmap_cvs = bitmap_cvs_map[bitmap_type];

  // put bitmap
  bitmap_cache->produceBitmap(key, bitmap, valid);

  bitmap_mutex->lock();
  auto cvIt = bitmap_cvs.find(key);
  if (cvIt != bitmap_cvs.end()) {
    cvIt->second->notify_one();
  }
  bitmap_mutex->unlock();
}

void FlightHandler::init_bitmap_cache() {
  auto bitmap_types = {BitmapType::FILTER_COMPUTE, BitmapType::FILTER_STORAGE, BitmapType::BLOOM_FILTER_COMPUTE};

  for (auto bitmap_type: bitmap_types) {
    bitmap_cache_map[bitmap_type] = std::make_shared<BitmapCache>();
    bitmap_mutex_map[bitmap_type] = std::make_shared<std::mutex>();
    bitmap_cvs_map[bitmap_type] = std::unordered_map<std::string, std::shared_ptr<std::condition_variable_any>>();
  }
}

} // namespace fpdb::store::server::flight