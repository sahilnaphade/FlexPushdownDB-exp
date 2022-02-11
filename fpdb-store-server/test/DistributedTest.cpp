//
// Created by matt on 11/2/22.
//

#include <doctest/doctest.h>

#include "Global.hpp"

#include "fpdb/store/server/Server.hpp"

using namespace fpdb::store::server;
using namespace fpdb::store::server::flight;
using namespace fpdb::store::server::caf;

TEST_SUITE("fpdb-store-server/DistributedTest" * doctest::skip(false)) {

  TEST_CASE("fpdb-store-server/DistributedTest/boot" * doctest::skip(false)) {

    // Start server 1
    auto server1 = Server::make("1", 0, true, 0, std::nullopt,std::nullopt, std::nullopt, 0, actor_manager);
    auto init_result1 = server1->init();
    REQUIRE(init_result1.has_value());
    auto start_result1 = server1->start();
    REQUIRE(start_result1.has_value());

    // Start server 2
    auto server2 = Server::make("2", 0, false, std::nullopt,server1->cluster_actor_handle(),
                                std::nullopt, std::nullopt, 0,
                                actor_manager);
    auto init_result2 = server2->init();
    REQUIRE(init_result2.has_value());
    auto start_result2 = server2->start();
    REQUIRE(start_result2.has_value());

    server1->stop();
    server2->stop();
  }
}