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

#pragma once

#include <rados/librados.hpp>
#include <flatbuffers/flatbuffers.h>

#include "arrow/schema.h"
#include "arrow/table.h"
#include "arrow/record_batch.h"
#include "arrow/ipc/api.h"
#include "arrow/io/api.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/util/macros.h"
#include "arrow/dataset/scanner.h"
#include "generated/ScanRequest_generated.h"

namespace arrow {
namespace util {

class ARROW_DS_EXPORT IoCtxInterface {
 public:
  IoCtxInterface() {}

  /// \brief Write data to an object.
  ///
  /// \param[in] oid the ID of the object to write.
  /// \param[in] bl a bufferlist containing the data to write to the object.
  virtual int write_full(const std::string& oid, ceph::bufferlist& bl) = 0;

  /// \brief Read a RADOS object.
  ///
  /// \param[in] oid the object ID which to read.
  /// \param[in] bl a bufferlist to hold the contents of the read object.
  /// \param[in] len the length of data to read from an object.
  /// \param[in] offset the offset of the object to read from.
  virtual int read(const std::string& oid, ceph::bufferlist& bl, size_t len,
                   uint64_t offset) = 0;

  /// \brief Executes a CLS function.
  ///
  /// \param[in] oid the object ID on which to execute the CLS function.
  /// \param[in] cls the name of the CLS.
  /// \param[in] method the name of the CLS function.
  /// \param[in] in a bufferlist to send data to the CLS function.
  /// \param[in] out a bufferlist to recieve data from the CLS function.
  virtual int exec(const std::string& oid, const char* cls, const char* method,
                   ceph::bufferlist& in, ceph::bufferlist& out) = 0;

  virtual std::vector<std::string> list() = 0;

  virtual int stat(const std::string& oid, uint64_t* psize) = 0;

 private:
  friend class RadosWrapper;
  /// \brief Set the `librados::IoCtx` instance inside a IoCtxInterface instance.
  virtual void setIoCtx(librados::IoCtx* ioCtx_) = 0;
};

class ARROW_DS_EXPORT IoCtxWrapper : public IoCtxInterface {
 public:
  IoCtxWrapper() { ioCtx = new librados::IoCtx(); }
  ~IoCtxWrapper() { delete ioCtx; }
  int write_full(const std::string& oid, ceph::bufferlist& bl) override;
  int read(const std::string& oid, ceph::bufferlist& bl, size_t len,
           uint64_t offset) override;
  int exec(const std::string& oid, const char* cls, const char* method,
           ceph::bufferlist& in, ceph::bufferlist& out) override;
  std::vector<std::string> list() override;

  int stat(const std::string& oid, uint64_t* psize) override;

 private:
  void setIoCtx(librados::IoCtx* ioCtx_) override { *ioCtx = *ioCtx_; }
  librados::IoCtx* ioCtx;
};

class ARROW_DS_EXPORT RadosInterface {
 public:
  RadosInterface() {}

  /// \brief Initializes a cluster handle.
  ///
  /// \param[in] name the username of the client.
  /// \param[in] clustername the name of the Ceph cluster.
  /// \param[in] flags some extra flags to pass.
  virtual int init2(const char* const name, const char* const clustername,
                    uint64_t flags) = 0;

  /// \brief Create an I/O context
  ///
  /// \param[in] name the RADOS pool to connect to.
  /// \param[in] pioctx an instance of IoCtxInterface.
  virtual int ioctx_create(const char* name, IoCtxInterface* pioctx) = 0;

  /// \brief Read the Ceph config file.
  ///
  /// \param[in] path the path to the config file.
  virtual int conf_read_file(const char* const path) = 0;

  /// \brief Connect to the Ceph cluster.
  virtual int connect() = 0;

  /// \brief Close connection to the Ceph cluster.
  virtual void shutdown() = 0;
};

class ARROW_DS_EXPORT RadosWrapper : public RadosInterface {
 public:
  RadosWrapper() { cluster = new librados::Rados(); }
  ~RadosWrapper() { delete cluster; }
  int init2(const char* const name, const char* const clustername,
            uint64_t flags) override;
  int ioctx_create(const char* name, IoCtxInterface* pioctx) override;
  int conf_read_file(const char* const path) override;
  int connect() override;
  void shutdown() override;

 private:
  librados::Rados* cluster;
};

ARROW_DS_EXPORT Status SerializeScanRequest(std::shared_ptr<arrow::dataset::ScanOptions>& options,
                                            int& file_format, int64_t& file_size,
                                            ceph::bufferlist& bl);

ARROW_DS_EXPORT Status DeserializeScanRequest(compute::Expression* filter,
                                              compute::Expression* partition,
                                              std::shared_ptr<Schema>* projected_schema,
                                              std::shared_ptr<Schema>* dataset_schema,
                                              int64_t& file_size, int& file_format,
                                              ceph::bufferlist& bl);

ARROW_DS_EXPORT Status SerializeTable(std::shared_ptr<Table>& table, ceph::bufferlist& bl,
                                      bool aggressive = false);

ARROW_DS_EXPORT Status DeserializeTable(RecordBatchVector& batches, ceph::bufferlist& bl,
                                        bool use_threads);

}  // namespace util
}  // namespace arrow
