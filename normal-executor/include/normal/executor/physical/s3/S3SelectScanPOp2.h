//
// Created by matt on 14/8/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3SELECTSCANPOP2_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3SELECTSCANPOP2_H

#include <normal/executor/physical/s3/S3SelectScanKernel.h>
#include <normal/executor/physical/s3/S3SelectCSVParseOptions.h>
#include <normal/executor/physical/Forward.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/physical/POpActor2.h>
#include <normal/executor/message/Envelope.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/ScanMessage.h>
#include <normal/catalogue/Table.h>
#include <normal/aws/AWSClient.h>
#include <normal/tuple/FileType.h>
#include <memory>
#include <string>
#include <aws/s3/S3Client.h>
#include <caf/all.hpp>

using namespace normal::executor::message;
using namespace normal::catalogue;
using namespace normal::aws;
using namespace normal::tuple;

namespace normal::executor::physical::s3 {

class S3SelectScanPOp2 : public PhysicalOp {

public:

  S3SelectScanPOp2(std::string name,
				const std::string &s3Bucket,
				const std::string &s3Object,
				const std::string &sql,
				std::optional<int64_t> startOffset,
				std::optional<int64_t> finishOffset,
				FileType fileType,
        const std::optional<S3SelectCSVParseOptions> &parseOptions,
				std::optional<std::vector<std::string>> columnNames,
				const std::shared_ptr<Table>& table,
				const std::shared_ptr<AWSClient> &awsClient,
				bool scanOnStart,
				long queryId);

  static std::shared_ptr<S3SelectScanPOp2> make(const std::string &name,
											 const std::string &s3Bucket,
											 const std::string &s3Object,
											 const std::string &sql,
											 std::optional<int64_t> startOffset,
											 std::optional<int64_t> finishOffset,
											 FileType fileType,
                       const std::optional<S3SelectCSVParseOptions> &parseOptions,
											 const std::optional<std::vector<std::string>> &columnNames,
											 const std::shared_ptr<Table>& table,
											 const std::shared_ptr<AWSClient> &awsClient,
											 bool scanOnStart,
											 long queryId = 0);

  void onReceive(const Envelope &message) override;

private:
  // Column names to scan when scanOnStart_ is true
  std::optional<std::vector<std::string>> columnNames_;
  bool scanOnStart_;
  std::unique_ptr<S3SelectScanKernel> kernel_;
  // Column names to scan when receiving a scanmessage
  std::optional<std::vector<std::string>> columnNamesToScan_;

  [[nodiscard]] tl::expected<void, std::string> onStart();
  void onComplete(const CompleteMessage &message);
  void onScan(const ScanMessage &Message);

  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
  void readAndSendTuples(const std::vector<std::string> &columnNames);
};

using GetMetricsAtom = caf::atom_constant<caf::atom("g-metrics")>;

//using S3SelectScanActor = OperatorActor2::extend_with<::caf::typed_actor<
//	caf::reacts_to<ScanAtom, std::vector<std::string>, bool>>>;
//
//using S3SelectScanStatefulActor = S3SelectScanActor::stateful_pointer<S3SelectScanState>;
//
//class S3SelectScanState : public OperatorActorState<S3SelectScanStatefulActor> {
//public:
//  void setState(S3SelectScanStatefulActor self,
//				const char *name_,
//				const std::string &s3Bucket,
//				const std::string &s3Object,
//				const std::string &sql,
//				std::optional<int64_t> startOffset,
//				std::optional<int64_t> finishOffset,
//				FileType fileType,
//				const std::optional<std::vector<std::string>> &columnNames,
//				const std::optional<S3SelectCSVParseOptions> &parseOptions,
//				const std::shared_ptr<S3Client> &s3Client,
//				bool scanOnStart) {
//
//	OperatorActorState::setBaseState(self, name_);
//
//	legacyOperator_ = S3SelectScan2::make(name_,
//										  s3Bucket,
//										  s3Object,
//										  sql,
//										  startOffset,
//										  finishOffset,
//										  fileType,
//										  columnNames,
//										  parseOptions,
//										  s3Client,
//										  scanOnStart);
//  }
//
//  template<class... Handlers>
//  S3SelectScanActor::behavior_type makeBehavior(S3SelectScanStatefulActor self, Handlers... handlers) {
//	return OperatorActorState::makeBaseBehavior(
//		self,
//		[=](ScanAtom, const std::vector<std::string> &columnNames, bool resultNeeded) {
//		  SPDLOG_DEBUG("[Actor {} ('{}')]  Scan  |  sender: {}", self->id(),
//					   self->name(), to_string(self->current_sender()));
//		  self->state.legacyOperator_->onReceive(Envelope(std::make_shared<ScanMessage>(columnNames,
//																						to_string(self->current_sender()),
//																						resultNeeded)));
//		},
//		std::move(handlers)...
//	);
//  }
//
//  static S3SelectScanActor::behavior_type spawnFunctor(S3SelectScanStatefulActor self,
//													   const char *name_,
//													   const std::string &s3Bucket,
//													   const std::string &s3Object,
//													   const std::string &sql,
//													   std::optional<int64_t> startOffset,
//													   std::optional<int64_t> finishOffset,
//													   FileType fileType,
//													   const std::optional<std::vector<std::string>> &columnNames,
//													   const std::optional<S3SelectCSVParseOptions> &parseOptions,
//													   const std::shared_ptr<S3Client> &s3Client,
//													   bool scanOnStart) {
//	self->state.setState(self,
//						 name_,
//						 s3Bucket,
//						 s3Object,
//						 sql,
//						 startOffset,
//						 finishOffset,
//						 fileType,
//						 columnNames,
//						 parseOptions,
//						 s3Client,
//						 scanOnStart);
//
//	return self->state.makeBehavior(self);
//  }
//
//private:
//  std::shared_ptr<S3SelectScan2> legacyOperator_;
//
//};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3SELECTSCANPOP2_H
