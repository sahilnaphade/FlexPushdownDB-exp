//
// Created by matt on 16/6/20.
//

#ifndef NORMAL_NORMAL_AWS_INCLUDE_NORMAL_AWS_S3UTIL_H
#define NORMAL_NORMAL_AWS_INCLUDE_NORMAL_AWS_S3UTIL_H

#include <aws/s3/S3Client.h>
#include <string>

using namespace Aws::S3;
using namespace std;

namespace normal::aws {

class S3Util {

public:

  /**
   * A utility for getting the sizes of a bunch of s3 objects
   *
   * TODO: This is just the beginnings of a method of getting partition metadata for splitting up work
   *  across partitions. At the moment, just using it to get object sizes.
   */
  static unordered_map<string, long> listObjects(const string& s3Bucket,
                                                 const string& prefix,
                                                 const vector<string>& s3Objects,
                                                 const shared_ptr<S3Client>& s3Client);

};

}

#endif //NORMAL_NORMAL_AWS_INCLUDE_NORMAL_AWS_S3UTIL_H
