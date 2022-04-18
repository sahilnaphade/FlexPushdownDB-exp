//
// Created by Yifei Yang on 2/23/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_UTIL_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_UTIL_HPP

#include "string"

namespace fpdb::store::server::flight {

static constexpr std::string_view TypeJSONName = "type";
static constexpr std::string_view BucketJSONName = "bucket";
static constexpr std::string_view ObjectJSONName = "object";
static constexpr std::string_view QueryIdJSONName = "query_id";
static constexpr std::string_view QueryPlanJSONName = "query_plan";
static constexpr std::string_view OpJSONName = "op";
static constexpr std::string_view ValidJSONName = "valid";

static constexpr std::string_view GetObjectCmdTypeName = "get-object";
static constexpr std::string_view SelectObjectContentCmdTypeName = "select-object-content";
static constexpr std::string_view PutBitmapCmdTypeName = "put-bitmap";
static constexpr std::string_view GetObjectTicketTypeName = "get-object";
static constexpr std::string_view SelectObjectContentTicketTypeName = "select-object-content";
static constexpr std::string_view GetBitmapTicketTypeName = "get-bitmap";

static constexpr std::string_view HeaderMiddlewareKey = "header-middleware";
static constexpr std::string_view BucketHeaderKey = "bucket";
static constexpr std::string_view ObjectHeaderKey = "object";

}

#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_UTIL_HPP
