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
#include <rados/objclass.h>

#include <memory>

// added
#include <sys/sysinfo.h>
//#include "arrow/dataset/file_skyhook.h"
#include "arrow/dataset/scanner.h"
#include <string>
#include <boost/utility/string_view.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/tuple/tuple.hpp>
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include <sstream>
#include <iostream>
#include <string_view>
#include "arrow/util/logging.h"
#include <thread>
#include <assert.h>
#include <sched.h>
#include <stdbool.h>
#include <unistd.h>
//

#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/util/logging.h"

#include "skyhook/protocol/skyhook_protocol.h"

CLS_VER(1, 0)
CLS_NAME(skyhook)

//added
// MIN_AVAIL_RAM
#define MIN_AVAIL_RAM 0
#define MAX_CPU_UTIL 0
//

cls_handle_t h_class;
cls_method_handle_t h_scan_op;

/// \brief Log skyhook errors using RADOS object class SDK's logger.
void LogSkyhookError(const std::string& msg) { CLS_LOG(0, "error: %s", msg.c_str()); }

/// \class RandomAccessObject
/// \brief An interface to provide a file-like view over RADOS objects.
class RandomAccessObject : public arrow::io::RandomAccessFile {
 public:
  explicit RandomAccessObject(cls_method_context_t hctx, int64_t file_size) {
    hctx_ = hctx;
    content_length_ = file_size;
    chunks_ = std::vector<std::shared_ptr<ceph::bufferlist>>();
  }

  ~RandomAccessObject() override { DCHECK_OK(Close()); }

  /// Check if the file stream is closed.
  arrow::Status CheckClosed() const {
    if (closed_) {
      return arrow::Status::Invalid("Operation on closed stream");
    }
    return arrow::Status::OK();
  }

  /// Check if the position of the object is valid.
  arrow::Status CheckPosition(int64_t position, const char* action) const {
    if (position < 0) {
      return arrow::Status::Invalid("Cannot ", action, " from negative position");
    }
    if (position > content_length_) {
      return arrow::Status::IOError("Cannot ", action, " past end of file");
    }
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    return arrow::Status::NotImplemented(
        "ReadAt has not been implemented in RandomAccessObject");
  }

  /// Read a specified number of bytes from a specified position.
  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                       int64_t nbytes) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // No need to allocate more than the remaining number of bytes
    nbytes = std::min(nbytes, content_length_ - position);

    if (nbytes > 0) {
      std::shared_ptr<ceph::bufferlist> bl = std::make_shared<ceph::bufferlist>();
      cls_cxx_read(hctx_, position, nbytes, bl.get());
      chunks_.push_back(bl);
      return std::make_shared<arrow::Buffer>((uint8_t*)bl->c_str(), bl->length());
    }
    return std::make_shared<arrow::Buffer>("");
  }

  /// Read a specified number of bytes from the current position.
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(pos_, nbytes));
    pos_ += buffer->size();
    return std::move(buffer);
  }

  /// Read a specified number of bytes from the current position into an output stream.
  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(pos_, nbytes, out));
    pos_ += bytes_read;
    return bytes_read;
  }

  /// Return the size of the file.
  arrow::Result<int64_t> GetSize() override {
    RETURN_NOT_OK(CheckClosed());
    return content_length_;
  }

  /// Sets the file-pointer offset, measured from the beginning of the
  /// file, at which the next read or write occurs.
  arrow::Status Seek(int64_t position) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return arrow::Status::OK();
  }

  /// Returns the file-pointer offset.
  arrow::Result<int64_t> Tell() const override {
    RETURN_NOT_OK(CheckClosed());
    return pos_;
  }

  /// Mark the file as closed.
  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }

  bool closed() const override { return closed_; }

 private:
  cls_method_context_t hctx_;
  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = -1;
  std::vector<std::shared_ptr<ceph::bufferlist>> chunks_;
};

/// \brief Driver function to execute the Scan operations.
/// \param[in] hctx RADOS object context.
/// \param[in] req The scan request received from the client.
/// \param[in] format The file format instance to use in the scan.
/// \param[in] fragment_scan_options The fragment scan options to use to customize the
/// scan.
/// \return Table.
arrow::Result<std::shared_ptr<arrow::Table>> DoScan(
    cls_method_context_t hctx, const skyhook::ScanRequest& req,
    const std::shared_ptr<arrow::dataset::FileFormat>& format,
    const std::shared_ptr<arrow::dataset::FragmentScanOptions>& fragment_scan_options) {
  auto file = std::make_shared<RandomAccessObject>(hctx, req.file_size);
  arrow::dataset::FileSource source(file);
  ARROW_ASSIGN_OR_RAISE(
      auto fragment, format->MakeFragment(std::move(source), req.partition_expression));
  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  auto builder = std::make_shared<arrow::dataset::ScannerBuilder>(
      req.dataset_schema, std::move(fragment), std::move(options));

  ARROW_RETURN_NOT_OK(builder->Filter(req.filter_expression));
  ARROW_RETURN_NOT_OK(builder->Project(req.projection_schema->field_names()));
  ARROW_RETURN_NOT_OK(builder->UseThreads(true));
  ARROW_RETURN_NOT_OK(builder->FragmentScanOptions(fragment_scan_options));

  ARROW_ASSIGN_OR_RAISE(auto scanner, builder->Finish());
  ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());
  return table;
}

/// \brief Scan RADOS objects containing Arrow IPC data.
/// \param[in] hctx The RADOS object context.
/// \param[in] req The scan request received from the client.
/// \return Table.
static arrow::Result<std::shared_ptr<arrow::Table>> ScanIpcObject(
    cls_method_context_t hctx, skyhook::ScanRequest req) {
  auto format = std::make_shared<arrow::dataset::IpcFileFormat>();
  auto fragment_scan_options = std::make_shared<arrow::dataset::IpcFragmentScanOptions>();

  ARROW_ASSIGN_OR_RAISE(auto result_table, DoScan(hctx, req, std::move(format),
                                                  std::move(fragment_scan_options)));
  return result_table;
}

/// \brief Scan RADOS objects containing Parquet binary data.
/// \param[in] hctx The RADOS object context.
/// \param[in] req The scan request received from the client.
/// \return Table.
static arrow::Result<std::shared_ptr<arrow::Table>> ScanParquetObject(
    cls_method_context_t hctx, skyhook::ScanRequest req) {
  auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
  auto fragment_scan_options =
      std::make_shared<arrow::dataset::ParquetFragmentScanOptions>();

  ARROW_ASSIGN_OR_RAISE(auto result_table, DoScan(hctx, req, std::move(format),
                                                  std::move(fragment_scan_options)));
  return result_table;
}

static void MaybePushback(const cls_method_context_t& hctx,
    int64_t file_size, ceph::bufferlist *out)
{
  struct sysinfo info;
  ARROW_CHECK_LT(sysinfo(&info), 0) << "Fail to get sysinfo: " << std::strerror(errno);

  auto parquet_reader = parquet::ParquetFileReader::Open(
      std::make_shared<RandomAccessObject>(hctx, file_size));
  auto file_metadata = parquet_reader->metadata();

  int64_t file_uncompressed_size = 0;
  for (int r = 0; r < file_metadata->num_row_groups(); ++r) {
    auto row_group_metadata = file_metadata->RowGroup(r);
    for (int c = 0; c < file_metadata->num_columns(); ++c) {
      file_uncompressed_size += row_group_metadata->ColumnChunk(c)->total_uncompressed_size();
    }
  }

  CLS_LOG(0, "#### file_uncompressed_size %lu", file_uncompressed_size);

  // TODO: Use free ram in sysinfo instead of MIN_AVAIL_RAM
  if (file_uncompressed_size > MIN_AVAIL_RAM) {
    cls_cxx_read(hctx, 0, file_size, out);
    return;
  }

  const auto processor_count = std::thread::hardware_concurrency();
  cpu_set_t mask;
  long nproc;
  nproc = sysconf(_SC_NPROCESSORS_ONLN);

  int num_cpu_avail = 0;
  for (int i = 0; i < nproc; i++) {
      //CLS_LOG(0, "#### %d ", CPU_ISSET(i, &mask));
      num_cpu_avail +=  CPU_ISSET(i, &mask);
  }
  CLS_LOG(0, "#### total affinity: %d ", num_cpu_avail);
  CLS_LOG(0, "#### \n");
  CLS_LOG(0, "#### sched_getcpu = %d\n", sched_getcpu());


  // TODO: get real info loads[0] value. Google this
  // start benchmarking. Use parsec

  float f_load = 1.f / (1 << SI_LOAD_SHIFT);

  // TODO: compare these values with the uptime result

  CLS_LOG(0, "#### info.loads[0] 1 %f", info.loads[0] * f_load);
  CLS_LOG(0, "#### info.loads[0] 2 %f", info.loads[0] * f_load * 100/nproc);
  CLS_LOG(0, "#### processor count %d", processor_count);


  if (info.loads[0] > MAX_CPU_UTIL) {
    cls_cxx_read(hctx, 0, file_size, out);
  }
}

/// \brief The scan operation to execute on the Ceph OSD nodes. The scan request is
/// deserialized, the object is scanned, and the resulting table is serialized
/// and sent back to the client.
/// \param[in] hctx The RADOS object context.
/// \param[in] in A bufferlist containing serialized Scan request.
/// \param[out] out A bufferlist to store the serialized resultant table.
/// \return Exit code.
static int scan_op(cls_method_context_t hctx, ceph::bufferlist* in,
                   ceph::bufferlist* out) {
  // Components required to construct a File fragment.
  arrow::Status s;
  skyhook::ScanRequest req;

  auto pushback_policy = req.pushback_policy;
  //CLS_LOG(0, "#### pushback policy type %d", pushback_policy);

  // Deserialize the scan request.
  if (!(s = skyhook::DeserializeScanRequest(*in, &req)).ok()) {
    LogSkyhookError(s.message());
    return SCAN_REQ_DESER_ERR_CODE;
  }

  if (pushback_policy == skyhook::SkyhookFragmentScanOptions::pushback_policy_type::ALWAYS) {
    cls_cxx_read(hctx, 0, req.file_size, out);
  } else if (pushback_policy == skyhook::SkyhookFragmentScanOptions::pushback_policy_type::DYNAMIC) {
    MaybePushback(hctx, req.file_size, out);
  }

  if (out->length()) {
    return SCAN_RES_PUSHBACK;
  }


  // Scan the object.
  std::shared_ptr<arrow::Table> table;
  arrow::Result<std::shared_ptr<arrow::Table>> maybe_table;
  switch (req.file_format) {
    case skyhook::SkyhookFileType::type::PARQUET:
      maybe_table = ScanParquetObject(hctx, std::move(req));
      if (!maybe_table.ok()) {
        LogSkyhookError("Could not scan parquet object: " +
                        maybe_table.status().ToString());
        return SCAN_ERR_CODE;
      }
      table = *maybe_table;
      break;
    case skyhook::SkyhookFileType::type::IPC:
      maybe_table = ScanIpcObject(hctx, std::move(req));
      if (!maybe_table.ok()) {
        LogSkyhookError("Could not scan IPC object: " + maybe_table.status().ToString());
        return SCAN_ERR_CODE;
      }
      table = *maybe_table;
      break;
    default:
      table = nullptr;
  }
  if (!table) {
    LogSkyhookError("Unsupported file format");
    return SCAN_ERR_CODE;
  }

  // Serialize the resultant table to send back to the client.
  if (!(s = skyhook::SerializeTable(table, out)).ok()) {
    LogSkyhookError(s.message());
    return SCAN_RES_SER_ERR_CODE;
  }

  return 0;
}

void __cls_init() {
  /// Register the skyhook object classes with the OSD.
  cls_register("skyhook", &h_class);
  cls_register_cxx_method(h_class, "scan_op", CLS_METHOD_RD, scan_op, &h_scan_op);
}
