//
// Created by matt on 5/1/20.
//

#include <normal/executor/message/StartMessage.h>
#include <utility>

namespace normal::executor::message {

StartMessage::StartMessage(
                           std::string from) :
    Message("StartMessage", std::move(from)){

}

}
