//
// Created by matt on 11/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_SERVERMETA_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_SERVERMETA_HPP

#include <caf/all.hpp>

CAF_BEGIN_TYPE_ID_BLOCK(Server, ::caf::first_custom_type_id)

// Event fired when server coming online
CAF_ADD_ATOM(Server, ServerUp)

// Event fired when server goes offline (may not be sent due to crash of course)
CAF_ADD_ATOM(Server, ServerDown)

CAF_END_TYPE_ID_BLOCK(Server)

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_SERVERMETA_HPP
