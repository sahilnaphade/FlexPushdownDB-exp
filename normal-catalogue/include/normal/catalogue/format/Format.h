//
// Created by Yifei Yang on 11/11/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_FORMAT_FORMAT_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_FORMAT_FORMAT_H

#include <normal/catalogue/format/FormatType.h>

namespace normal::catalogue::format {

class Format {
public:
  Format(FormatType type);
  Format() = default;
  Format(const Format&) = default;
  Format& operator=(const Format&) = default;

  FormatType getType() const;

protected:
  FormatType type_;
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_FORMAT_FORMAT_H
