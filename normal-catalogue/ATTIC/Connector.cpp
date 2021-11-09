//
// Created by matt on 27/3/20.
//

#include "../ATTIC/Connector.h"

#include <utility>

normal::connector::Connector::Connector(std::string name) :
    name_(std::move(name)) {
}
