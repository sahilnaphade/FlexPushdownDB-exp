//
// Created by matt on 4/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_FLIGHTHANDLER_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_FLIGHTHANDLER_HPP

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <tl/expected.hpp>
#include "mutex"
#include "condition_variable"

#include "fpdb/store/server/flight/SelectObjectContentCmd.hpp"
#include "fpdb/store/server/flight/PutBitmapCmd.hpp"
#include "fpdb/store/server/flight/ClearBitmapCmd.hpp"
#include "fpdb/store/server/flight/GetObjectTicket.hpp"
#include "fpdb/store/server/flight/SelectObjectContentTicket.hpp"
#include "fpdb/store/server/flight/GetBitmapTicket.hpp"
#include "fpdb/store/server/flight/GetTableTicket.hpp"
#include "fpdb/store/server/flight/HeaderMiddleware.hpp"
#include "fpdb/store/server/flight/BitmapType.h"
#include "fpdb/store/server/caf/ActorManager.hpp"
#include "fpdb/store/server/cache/BitmapCache.hpp"
#include "fpdb/store/server/cache/TableCache.hpp"

using namespace fpdb::store::server::cache;
using namespace ::arrow::flight;

namespace fpdb::store::server::flight {

class FlightHandler : FlightServerBase {

public:
  /**
   *
   * @param location
   */
  explicit FlightHandler(Location location,
                         std::string store_root_path,
                         std::shared_ptr<::caf::actor_system> actor_system);

  /**
   *
   */
  ~FlightHandler() override;

  /**
   *
   * @return
   */
  tl::expected<void, std::string> init();

  /**
   *
   * @return
   */
  tl::expected<void, std::string> serve();

  /**
   *
   * @return
   */
  tl::expected<void, std::string> shutdown();

  /**
   *
   * @return
   */
  tl::expected<void, std::string> wait();

  /**
   *
   * @return
   */
  int port();

  /**
   *
   * @param context
   * @param request
   * @param info
   * @return
   */
  ::arrow::Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                                std::unique_ptr<FlightInfo>* info) override;

  /**
   *
   * @param context
   * @param request
   * @param stream
   * @return
   */
  ::arrow::Status DoGet(const ServerCallContext& context, const Ticket& request,
                        std::unique_ptr<FlightDataStream>* stream) override;

  /**
   *
   * @param context
   * @param reader
   * @param writer
   * @return
   */
  ::arrow::Status DoPut(const ServerCallContext& context,
                        std::unique_ptr<FlightMessageReader> reader,
                        std::unique_ptr<FlightMetadataWriter> writer) override;

private:
  /**
   *
   * @param context
   * @return
   */
  static tl::expected<HeaderMiddleware*, std::string> get_header_middleware(const ServerCallContext& context);

  /**
   *
   * @param key
   * @param middleware
   * @return
   */
  static tl::expected<std::string, std::string> parse_header(const std::string& key,
                                                             const HeaderMiddleware& middleware);

  /**
   *
   * @param context
   * @param request
   * @return
   */
  tl::expected<std::unique_ptr<FlightInfo>, ::arrow::Status> get_flight_info(const ServerCallContext& context,
                                                                             const FlightDescriptor& request);

  /**
   *
   * @param context
   * @param request
   * @param bucket
   * @param object
   * @return
   */
  static tl::expected<std::unique_ptr<FlightInfo>, ::arrow::Status>
  get_flight_info_for_path(const ServerCallContext& context, const FlightDescriptor& request, std::string bucket,
                           std::string object);

  /**
   *
   * @param context
   * @param request
   * @param bucket
   * @param object
   * @return
   */
  tl::expected<std::unique_ptr<FlightInfo>, ::arrow::Status>
  get_flight_info_for_cmd(const ServerCallContext& context, const FlightDescriptor& request, std::string bucket,
                          std::string object);

  /**
   *
   * @param context
   * @param request
   * @param bucket
   * @param object
   * @param select_object_content_cmd
   * @return
   */
  static tl::expected<std::unique_ptr<FlightInfo>, ::arrow::Status> get_flight_info_for_select_object_content_cmd(
    const ServerCallContext& context, const FlightDescriptor& request,
    const std::shared_ptr<SelectObjectContentCmd>& select_object_content_cmd);

  /**
   *
   * @param context
   * @param request
   * @return
   */
  tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status> do_get(const ServerCallContext& context,
                                                                          const Ticket& request);

  /**
   *
   * @param context
   * @param get_object_ticket
   * @return
   */
  tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status>
  do_get_get_object(const ServerCallContext& context,
                    const std::shared_ptr<GetObjectTicket>& get_object_ticket);

  /**
   *
   * @param context
   * @param select_object_content_ticket
   * @return
   */
  tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status>
  do_get_select_object_content(const ServerCallContext& context,
                               const std::shared_ptr<SelectObjectContentTicket>& select_object_content_ticket);

  /**
   *
   * @param context
   * @param get_bitmap_ticket
   * @return
   */
  tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status>
  do_get_get_bitmap(const ServerCallContext& context,
                    const std::shared_ptr<GetBitmapTicket>& get_bitmap_ticket);

  /**
   *
   * @param context
   * @param get_table_ticket
   * @return
   */
  tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status>
  do_get_get_table(const ServerCallContext& context,
                   const std::shared_ptr<GetTableTicket>& get_table_ticket);

  /**
   *
   * @param context
   * @param reader
   * @return
   */
  tl::expected<void, ::arrow::Status> do_put(const ServerCallContext& context,
                                             const std::unique_ptr<FlightMessageReader> &reader);

  /**
   *
   * @param context
   * @param reader
   * @return
   */
  tl::expected<void, ::arrow::Status> do_put_for_cmd(const ServerCallContext& context,
                                                     const std::unique_ptr<FlightMessageReader> &reader);

  /**
   *
   * @param context
   * @param query_id
   * @param op
   * @param reader
   * @return
   */
  tl::expected<void, ::arrow::Status> do_put_put_bitmap(const ServerCallContext& context,
                                                        const std::shared_ptr<PutBitmapCmd>& put_bitmap_cmd,
                                                        const std::unique_ptr<FlightMessageReader> &reader);

  /**
   *
   * @param context
   * @param clear_bitmap_cmd
   * @return
   */
  tl::expected<void, ::arrow::Status> do_put_clear_bitmap(const ServerCallContext& context,
                                                          const std::shared_ptr<ClearBitmapCmd>& clear_bitmap_cmd);

  /**
   *
   * @param key
   * @param bitmap_type
   * @return
   */
  std::optional<std::vector<int64_t>> get_bitmap_from_cache(const std::string &key,
                                                            BitmapType bitmap_type);

  /**
   *
   * @param key
   * @param bitmap
   * @param bitmap_type
   * @param valid
   */
  void put_bitmap_into_cache(const std::string &key,
                             const std::vector<int64_t> &bitmap,
                             BitmapType bitmap_type,
                             bool valid);

  /**
   * init bitmap caches, as well as mutex and cond_var used for them
   */
  void init_bitmap_cache();

  /**
   * Make record batches from table, generating at least 1 batch, which is required by record batch stream
   * @param table
   * @return
   */
  tl::expected<::arrow::RecordBatchVector, ::arrow::Status>
  table_to_record_batches(const std::shared_ptr<::arrow::Table> &table);

  ::arrow::flight::Location location_;
  std::string store_root_path_;
  std::shared_ptr<::caf::actor_system> actor_system_;

  // bitmap caches for different bitmap types
  std::unordered_map<BitmapType, std::shared_ptr<BitmapCache>> bitmap_cache_map;
  std::unordered_map<BitmapType, std::shared_ptr<std::mutex>> bitmap_mutex_map;
  std::unordered_map<BitmapType,
          std::unordered_map<std::string, std::shared_ptr<std::condition_variable_any>>> bitmap_cvs_map;

  // table cache (e.x. for shuffle result)
  TableCache table_cache_;
};

} // namespace fpdb::store::server::flight

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_FLIGHTHANDLER_HPP
