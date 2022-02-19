// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: FileService.proto

#include "FileService.pb.h"
#include "FileService.grpc.pb.h"

#include <functional>
#include <grpcpp/impl/codegen/async_stream.h>
#include <grpcpp/impl/codegen/async_unary_call.h>
#include <grpcpp/impl/codegen/channel_interface.h>
#include <grpcpp/impl/codegen/client_unary_call.h>
#include <grpcpp/impl/codegen/client_callback.h>
#include <grpcpp/impl/codegen/message_allocator.h>
#include <grpcpp/impl/codegen/method_handler.h>
#include <grpcpp/impl/codegen/rpc_service_method.h>
#include <grpcpp/impl/codegen/server_callback.h>
#include <grpcpp/impl/codegen/server_callback_handlers.h>
#include <grpcpp/impl/codegen/server_context.h>
#include <grpcpp/impl/codegen/service_type.h>
#include <grpcpp/impl/codegen/sync_stream.h>
namespace fpdb {
namespace store {
namespace server {
namespace file {

static const char* FileService_method_names[] = {
  "/fpdb.store.server.file.FileService/ReadFile",
  "/fpdb.store.server.file.FileService/GetFileSize",
};

std::unique_ptr< FileService::Stub> FileService::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< FileService::Stub> stub(new FileService::Stub(channel));
  return stub;
}

FileService::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel)
  : channel_(channel), rpcmethod_ReadFile_(FileService_method_names[0], ::grpc::internal::RpcMethod::SERVER_STREAMING, channel)
  , rpcmethod_GetFileSize_(FileService_method_names[1], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  {}

::grpc::ClientReader< ::fpdb::store::server::file::ReadFileResponse>* FileService::Stub::ReadFileRaw(::grpc::ClientContext* context, const ::fpdb::store::server::file::ReadFileRequest& request) {
  return ::grpc::internal::ClientReaderFactory< ::fpdb::store::server::file::ReadFileResponse>::Create(channel_.get(), rpcmethod_ReadFile_, context, request);
}

void FileService::Stub::experimental_async::ReadFile(::grpc::ClientContext* context, ::fpdb::store::server::file::ReadFileRequest* request, ::grpc::experimental::ClientReadReactor< ::fpdb::store::server::file::ReadFileResponse>* reactor) {
  ::grpc::internal::ClientCallbackReaderFactory< ::fpdb::store::server::file::ReadFileResponse>::Create(stub_->channel_.get(), stub_->rpcmethod_ReadFile_, context, request, reactor);
}

::grpc::ClientAsyncReader< ::fpdb::store::server::file::ReadFileResponse>* FileService::Stub::AsyncReadFileRaw(::grpc::ClientContext* context, const ::fpdb::store::server::file::ReadFileRequest& request, ::grpc::CompletionQueue* cq, void* tag) {
  return ::grpc::internal::ClientAsyncReaderFactory< ::fpdb::store::server::file::ReadFileResponse>::Create(channel_.get(), cq, rpcmethod_ReadFile_, context, request, true, tag);
}

::grpc::ClientAsyncReader< ::fpdb::store::server::file::ReadFileResponse>* FileService::Stub::PrepareAsyncReadFileRaw(::grpc::ClientContext* context, const ::fpdb::store::server::file::ReadFileRequest& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncReaderFactory< ::fpdb::store::server::file::ReadFileResponse>::Create(channel_.get(), cq, rpcmethod_ReadFile_, context, request, false, nullptr);
}

::grpc::Status FileService::Stub::GetFileSize(::grpc::ClientContext* context, const ::fpdb::store::server::file::GetFileSizeRequest& request, ::fpdb::store::server::file::GetFileSizeResponse* response) {
  return ::grpc::internal::BlockingUnaryCall< ::fpdb::store::server::file::GetFileSizeRequest, ::fpdb::store::server::file::GetFileSizeResponse, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_GetFileSize_, context, request, response);
}

void FileService::Stub::experimental_async::GetFileSize(::grpc::ClientContext* context, const ::fpdb::store::server::file::GetFileSizeRequest* request, ::fpdb::store::server::file::GetFileSizeResponse* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::fpdb::store::server::file::GetFileSizeRequest, ::fpdb::store::server::file::GetFileSizeResponse, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_GetFileSize_, context, request, response, std::move(f));
}

void FileService::Stub::experimental_async::GetFileSize(::grpc::ClientContext* context, const ::fpdb::store::server::file::GetFileSizeRequest* request, ::fpdb::store::server::file::GetFileSizeResponse* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_GetFileSize_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::fpdb::store::server::file::GetFileSizeResponse>* FileService::Stub::PrepareAsyncGetFileSizeRaw(::grpc::ClientContext* context, const ::fpdb::store::server::file::GetFileSizeRequest& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::fpdb::store::server::file::GetFileSizeResponse, ::fpdb::store::server::file::GetFileSizeRequest, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_GetFileSize_, context, request);
}

::grpc::ClientAsyncResponseReader< ::fpdb::store::server::file::GetFileSizeResponse>* FileService::Stub::AsyncGetFileSizeRaw(::grpc::ClientContext* context, const ::fpdb::store::server::file::GetFileSizeRequest& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncGetFileSizeRaw(context, request, cq);
  result->StartCall();
  return result;
}

FileService::Service::Service() {
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FileService_method_names[0],
      ::grpc::internal::RpcMethod::SERVER_STREAMING,
      new ::grpc::internal::ServerStreamingHandler< FileService::Service, ::fpdb::store::server::file::ReadFileRequest, ::fpdb::store::server::file::ReadFileResponse>(
          [](FileService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::fpdb::store::server::file::ReadFileRequest* req,
             ::grpc::ServerWriter<::fpdb::store::server::file::ReadFileResponse>* writer) {
               return service->ReadFile(ctx, req, writer);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FileService_method_names[1],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FileService::Service, ::fpdb::store::server::file::GetFileSizeRequest, ::fpdb::store::server::file::GetFileSizeResponse, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](FileService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::fpdb::store::server::file::GetFileSizeRequest* req,
             ::fpdb::store::server::file::GetFileSizeResponse* resp) {
               return service->GetFileSize(ctx, req, resp);
             }, this)));
}

FileService::Service::~Service() {
}

::grpc::Status FileService::Service::ReadFile(::grpc::ServerContext* context, const ::fpdb::store::server::file::ReadFileRequest* request, ::grpc::ServerWriter< ::fpdb::store::server::file::ReadFileResponse>* writer) {
  (void) context;
  (void) request;
  (void) writer;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status FileService::Service::GetFileSize(::grpc::ServerContext* context, const ::fpdb::store::server::file::GetFileSizeRequest* request, ::fpdb::store::server::file::GetFileSizeResponse* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}


}  // namespace fpdb
}  // namespace store
}  // namespace server
}  // namespace file

