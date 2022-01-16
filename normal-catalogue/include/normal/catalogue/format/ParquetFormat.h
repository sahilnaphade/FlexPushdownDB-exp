//
// Created by Yifei Yang on 11/11/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_FORMAT_PARQUETFORMAT_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_FORMAT_PARQUETFORMAT_H

#include <normal/catalogue/format/Format.h>

namespace normal::catalogue::format {

class ParquetFormat: public Format{
  
public:
  ParquetFormat();
  ParquetFormat(const ParquetFormat&) = default;
  ParquetFormat& operator=(const ParquetFormat&) = default;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ParquetFormat& format) {
    return f.apply(format.type_);
  }
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_FORMAT_PARQUETFORMAT_H
