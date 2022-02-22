//
// Created by Yifei Yang on 2/17/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_FILESERVICEHANDLER_H
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_FILESERVICEHANDLER_H

#include <FileService.grpc.pb.h>

using namespace grpc;
using namespace google::protobuf;

namespace fpdb::store::server::file {

class FileServiceHandler: public FileService::Service {

public:
  FileServiceHandler(const std::string &storeRootPath);
  ~FileServiceHandler() = default;

  Status ReadFile(ServerContext* context,
                  const ReadFileRequest* request,
                  ServerWriter<ReadFileResponse>* writer) override;

  Status GetFileSize(ServerContext* context,
                     const GetFileSizeRequest* request,
                     GetFileSizeResponse* response) override;

private:
  Status checkFileExist(const std::string &bucket, const std::string &object);
  void writeReadFileResponse(ServerWriter<ReadFileResponse>* writer, char *data, int64_t bytesRead);

  static constexpr int DefaultReadChunkSize = 16 * 1024 * 1024;   // 16 MB
  std::string storeRootPath_;
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_FILESERVICEHANDLER_H
