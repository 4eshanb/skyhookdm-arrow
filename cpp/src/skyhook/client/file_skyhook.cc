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

namespace skyhook {

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
        partition_expression_(partition_expression) {}

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

    /// Serialize the ScanRequest into a ceph bufferlist.
    ceph::bufferlist request;
    RETURN_NOT_OK(skyhook::SerializeScanRequest(req, request));

    /// Execute the Ceph object class method `scan_op`.
    ceph::bufferlist result;
    RETURN_NOT_OK(doa_->Exec(st.st_ino, "scan_op", request, result));

    /// Read RecordBatches from the result bufferlist. Since, this step might use
    /// threads for decompressing compressed batches, to avoid running into
    /// [ARROW-12597], we switch off threaded decompression to avoid nested threading
    /// scenarios when scan tasks are executed in parallel by the CpuThreadPool.
    arrow::RecordBatchVector batches;
    RETURN_NOT_OK(skyhook::DeserializeTable(batches, result, !options_->use_threads));
    return arrow::MakeVectorIterator(batches);
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
      : ctx_(std::move(ctx)), file_format_(file_format) {}

  ~Impl() {}

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
    file->handles_compute = false;

    /// Convert string file format name to Enum.
    skyhook::SkyhookFileType::type file_format;
    if (file_format_ == "parquet") {
      file_format = skyhook::SkyhookFileType::type::PARQUET;
    } else if (file_format_ == "ipc") {
      file_format = skyhook::SkyhookFileType::type::IPC;
    } else {
      return arrow::Status::Invalid("Unsupported file format.");
    }

    arrow::dataset::ScanTaskVector v{std::make_shared<SkyhookScanTask>(
        std::move(options), std::move(file), file->source(), std::move(doa_), file_format,
        file->partition_expression())};
    return arrow::MakeVectorIterator(v);
  }

  arrow::Result<std::shared_ptr<arrow::Schema>> Inspect(
      const arrow::dataset::FileSource& source) const {
    std::shared_ptr<arrow::dataset::FileFormat> file_format;
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

SkyhookFileFormat::SkyhookFileFormat(std::shared_ptr<RadosConnCtx> ctx,
                                     std::string file_format)
    : impl_(new Impl(std::move(ctx), file_format)) {}

SkyhookFileFormat::~SkyhookFileFormat() {}

arrow::Status SkyhookFileFormat::Init() { return impl_->Init(); }

arrow::Result<std::shared_ptr<arrow::Schema>> SkyhookFileFormat::Inspect(
    const arrow::dataset::FileSource& source) const {
  return impl_->Inspect(source);
}

arrow::Result<arrow::dataset::ScanTaskIterator> SkyhookFileFormat::ScanFile(
    const std::shared_ptr<arrow::dataset::ScanOptions>& options,
    const std::shared_ptr<arrow::dataset::FileFragment>& file) const {
  return impl_->ScanFile(std::move(options), std::move(file));
}

}  // namespace skyhook
