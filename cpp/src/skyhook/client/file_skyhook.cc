// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include "skyhook/client/file_skyhook.h"
#include "skyhook/protocol/rados_protocol.h"
#include "skyhook/protocol/skyhook_protocol.h"

#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/util/compression.h"

#include <iostream>
#include "arrow/io/api.h"
#include <flatbuffers/flatbuffers.h>
#include "arrow/ipc/api.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/dataset_internal.h"
//

namespace skyhook {
  //added
  using arrow::internal::checked_cast;
  //

/// A ScanTask to scan a file fragment in Skyhook format.
class SkyhookScanTask : public arrow::dataset::ScanTask {
 public:
  SkyhookScanTask(std::shared_ptr<arrow::dataset::ScanOptions> options,
                  std::shared_ptr<arrow::dataset::Fragment> fragment,
                  arrow::dataset::FileSource source,
                  std::shared_ptr<skyhook::SkyhookDirectObjectAccess> doa,
                  skyhook::SkyhookFileType::type file_format,
                  arrow::compute::Expression partition_expression)
      : ScanTask(std::move(options), std::move(fragment)),
        source_(std::move(source)),
        doa_(std::move(doa)),
        file_format_(file_format),
        partition_expression_(std::move(partition_expression)) {}

  arrow::Result<arrow::RecordBatchIterator> Execute() override {
    /// Retrieve the size of the file using POSIX `stat`.
    struct stat st {};
    RETURN_NOT_OK(doa_->Stat(source_.path(), st));

    /// Create a ScanRequest instance.
    skyhook::ScanRequest req;
    req.filter_expression = options_->filter;
    req.partition_expression = partition_expression_;
    req.projection_schema = options_->projected_schema;
    req.dataset_schema = options_->dataset_schema;
    req.file_size = st.st_size;
    req.file_format = file_format_;

    ARROW_ASSIGN_OR_RAISE(auto skyhook_fragment_scan_options,
                arrow::dataset::GetFragmentScanOptions<skyhook::SkyhookFragmentScanOptions>(skyhook::kSkyhookTypeName, options_.get(),
                std::make_shared<skyhook::SkyhookFragmentScanOptions>()));

    req.pushback_policy = skyhook_fragment_scan_options->pushback_policy();

    /// Serialize the ScanRequest into a ceph bufferlist.
    ceph::bufferlist request;
    RETURN_NOT_OK(skyhook::SerializeScanRequest(req, &request));

    /// Execute the Ceph object class method `scan_op`.
    ceph::bufferlist result;

    arrow::Status s = doa_->Exec(st.st_ino, "scan_op", request, result);
    auto& detail = checked_cast<const skyhook::rados::RadosStatusDetail&>(*s.detail());

    arrow::RecordBatchVector batches;

    if (detail.code() != SCAN_RES_PUSHBACK) {
      /// Read RecordBatches from the result bufferlist. Since, this step might use
      /// threads for decompressing compressed batches, to avoid running into
      /// [ARROW-12597], we switch off threaded decompression to avoid nested threading
      /// scenarios when scan tasks are executed in parallel by the CpuThreadPool.
      RETURN_NOT_OK(skyhook::DeserializeTable(result, !options_->use_threads, &batches));
      return arrow::MakeVectorIterator(std::move(batches));
    }

    auto buffer = std::make_shared<arrow::Buffer>((uint8_t*) result.c_str(), result.length());
    auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);

    auto source = arrow::dataset::FileSource(buffer);

    if (req.file_format == skyhook::SkyhookFileType::type::PARQUET) {
      auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
      auto fragment_scan_options = std::make_shared<arrow::dataset::ParquetFragmentScanOptions>();

      ARROW_ASSIGN_OR_RAISE(auto fragment, format->MakeFragment(source, req.partition_expression));
      auto options = std::make_shared<arrow::dataset::ScanOptions>();
      auto builder = std::make_shared<arrow::dataset::ScannerBuilder>(req.dataset_schema,
                      std::move(fragment), std::move(options));

      ARROW_RETURN_NOT_OK(builder->Filter(req.filter_expression));
      ARROW_RETURN_NOT_OK(builder->Project(req.projection_schema->field_names()));
      ARROW_RETURN_NOT_OK(builder->UseThreads(true));
      ARROW_RETURN_NOT_OK(builder->FragmentScanOptions(fragment_scan_options));

      ARROW_ASSIGN_OR_RAISE(auto scanner, builder->Finish());
      ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());

      auto options_read = arrow::ipc::IpcReadOptions::Defaults();
      options_read.use_threads = !options_->use_threads;

      ARROW_ASSIGN_OR_RAISE(auto reader, scanner->ToRecordBatchReader());
      ARROW_RETURN_NOT_OK(reader->ReadAll(&batches));

      return arrow::MakeVectorIterator(batches);
    } else if (req.file_format == skyhook::SkyhookFileType::type::IPC) {
      std::cout << "eh";
      return arrow::MakeVectorIterator(batches);
    } else {
      return arrow::Status::Invalid("Unsupported file format");
    }
  }

 protected:
  arrow::dataset::FileSource source_;
  std::shared_ptr<skyhook::SkyhookDirectObjectAccess> doa_;
  skyhook::SkyhookFileType::type file_format_;
  arrow::compute::Expression partition_expression_;
};

class SkyhookFileFormat::Impl {
 public:
  Impl(std::shared_ptr<RadosConnCtx> ctx, std::string file_format)
      : ctx_(std::move(ctx)), file_format_(std::move(file_format)) {}

  ~Impl() = default;

  arrow::Status Init() {
    /// Connect to the RADOS cluster and instantiate a `SkyhookDirectObjectAccess`
    /// instance.
    auto connection = std::make_shared<skyhook::rados::RadosConn>(ctx_);
    RETURN_NOT_OK(connection->Connect());
    doa_ = std::make_shared<skyhook::SkyhookDirectObjectAccess>(connection);
    return arrow::Status::OK();
  }

  arrow::Result<arrow::dataset::ScanTaskIterator> ScanFile(
      const std::shared_ptr<arrow::dataset::ScanOptions>& options,
      const std::shared_ptr<arrow::dataset::FileFragment>& file) const {
    /// Make sure client-side filtering and projection is turned off.
    file->apply_compute = false;

    /// Convert string file format name to Enum.
    skyhook::SkyhookFileType::type file_format;
    if (file_format_ == "parquet") {
      file_format = skyhook::SkyhookFileType::type::PARQUET;
    } else if (file_format_ == "ipc") {
      file_format = skyhook::SkyhookFileType::type::IPC;
    } else {
      return arrow::Status::Invalid("Unsupported file format ", file_format_);
    }

    arrow::dataset::ScanTaskVector v{std::make_shared<SkyhookScanTask>(
        options, file, file->source(), doa_, file_format, file->partition_expression())};
    return arrow::MakeVectorIterator(v);
  }

  arrow::Result<std::shared_ptr<arrow::Schema>> Inspect(
      const arrow::dataset::FileSource& source) const {
    std::shared_ptr<arrow::dataset::FileFormat> file_format;
    /// Convert string file format name to Arrow FileFormat.
    if (file_format_ == "parquet") {
      file_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    } else if (file_format_ == "ipc") {
      file_format = std::make_shared<arrow::dataset::IpcFileFormat>();
    } else {
      return arrow::Status::Invalid("Unsupported file format ", file_format_);
    }
    std::shared_ptr<arrow::Schema> schema;
    ARROW_ASSIGN_OR_RAISE(schema, file_format->Inspect(source));
    return schema;
  }

 private:
  std::shared_ptr<skyhook::SkyhookDirectObjectAccess> doa_;
  std::shared_ptr<RadosConnCtx> ctx_;
  std::string file_format_;
};

arrow::Result<std::shared_ptr<SkyhookFileFormat>> SkyhookFileFormat::Make(
    std::shared_ptr<RadosConnCtx> ctx, std::string file_format) {
  auto format =
      std::make_shared<SkyhookFileFormat>(std::move(ctx), std::move(file_format));
  /// Establish connection to the Ceph cluster.
  RETURN_NOT_OK(format->Init());
  return format;
}

SkyhookFileFormat::SkyhookFileFormat(std::shared_ptr<RadosConnCtx> ctx,
                                     std::string file_format)
    : impl_(new Impl(std::move(ctx), std::move(file_format))) {}

SkyhookFileFormat::~SkyhookFileFormat() = default;

arrow::Status SkyhookFileFormat::Init() { return impl_->Init(); }

arrow::Result<std::shared_ptr<arrow::Schema>> SkyhookFileFormat::Inspect(
    const arrow::dataset::FileSource& source) const {
  return impl_->Inspect(source);
}

arrow::Result<arrow::dataset::ScanTaskIterator> SkyhookFileFormat::ScanFile(
    const std::shared_ptr<arrow::dataset::ScanOptions>& options,
    const std::shared_ptr<arrow::dataset::FileFragment>& file) const {
  return impl_->ScanFile(options, file);
}

std::shared_ptr<arrow::dataset::FileWriteOptions>
SkyhookFileFormat::DefaultWriteOptions() {
  return nullptr;
}

arrow::Result<std::shared_ptr<arrow::dataset::FileWriter>> SkyhookFileFormat::MakeWriter(
    std::shared_ptr<arrow::io::OutputStream> destination,
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<arrow::dataset::FileWriteOptions> options,
    arrow::fs::FileLocator destination_locator) const {
  return arrow::Status::NotImplemented("Skyhook writer not yet implemented.");
}

}  // namespace skyhook
