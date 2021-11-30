//
// Created by Yifei Yang on 11/11/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_FORMAT_CSVFORMAT_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_FORMAT_CSVFORMAT_H

#include <normal/catalogue/format/Format.h>

namespace normal::catalogue::format {

class CSVFormat: public Format {
public:
  CSVFormat(char fieldDelimiter);

  char getFieldDelimiter() const;

private:
  char fieldDelimiter_;
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_FORMAT_CSVFORMAT_H
