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
#include "skyhook/protocol/rados_protocol.h"

#include "arrow/util/io_util.h"

#include <iostream>
#include <vector>
//added
#include <rados/objclass.h>
#include "arrow/status.h"
#include <sstream>
//

namespace skyhook {
namespace rados {

const char* kStatusDetailTypeId = "skyhook::rados::RadosStatusDetail";

RadosStatusDetail::RadosStatusDetail (int code): code_(code) {}
const char* RadosStatusDetail::type_id() const { return kStatusDetailTypeId; }

std::string RadosStatusDetail::ToString() const {
  std::stringstream ss;
  ss << "OK [code " << code_ << "]";
  return ss.str();
}

int RadosStatusDetail::code() const  { return code_; }

template <typename... Args>
arrow::Status StatusFromOK(int detail_code) {
  return arrow::Status::FromDetailAndArgs(arrow::StatusCode::OK,
         std::make_shared<RadosStatusDetail>(detail_code),
         "Rados operation success");
}

template <typename... Args>
arrow::Status GetStatusFromReturnCode(int code, Args&&... args) {
  if(code >= 0) {
    return StatusFromOK(code);
  } else {
    return arrow::internal::StatusFromErrno(code, arrow::StatusCode::Invalid, std::forward<Args>(args)...);
  }
}

arrow::Status IoCtxInterface::read(const std::string& oid, ceph::bufferlist& bl,
                                   size_t len, uint64_t offset) {
  return GetStatusFromReturnCode(ioCtx->read(oid, bl, len, offset),
                                 "ioctx->read failed.");
}

arrow::Status IoCtxInterface::exec(const std::string& oid, const char* cls,
                                   const char* method, ceph::bufferlist& in,
                                   ceph::bufferlist& out) {
  return GetStatusFromReturnCode(ioCtx->exec(oid, cls, method, in, out),
                                 "ioctx->exec failed.");
}

arrow::Status IoCtxInterface::stat(const std::string& oid, uint64_t* psize) {
  return GetStatusFromReturnCode(ioCtx->stat(oid, psize, NULL), "ioctx->stat failed.");
}

arrow::Status RadosInterface::init2(const char* const name, const char* const clustername,
                                    uint64_t flags) {
  return GetStatusFromReturnCode(cluster->init2(name, clustername, flags),
                                 "rados->init failed.");
}

arrow::Status RadosInterface::ioctx_create(const char* name, IoCtxInterface* pioctx) {
  librados::IoCtx ioCtx;
  int ret = cluster->ioctx_create(name, ioCtx);
  pioctx->setIoCtx(&ioCtx);
  return GetStatusFromReturnCode(ret, "rados->ioctx_create failed.");
}

arrow::Status RadosInterface::conf_read_file(const char* const path) {
  return GetStatusFromReturnCode(cluster->conf_read_file(path),
                                 "rados->conf_read_file failed.");
}

arrow::Status RadosInterface::connect() {
  return GetStatusFromReturnCode(cluster->connect(), "rados->connect failed.");
}

void RadosInterface::shutdown() { cluster->shutdown(); }

RadosConn::~RadosConn() { Shutdown(); }

arrow::Status RadosConn::Connect() {
  if (connected) {
    return arrow::Status::OK();
  }

  auto status = rados->init2(ctx->ceph_user_name.c_str(), ctx->ceph_cluster_name.c_str(), 0);
  ARROW_RETURN_IF(status.code() != arrow::StatusCode::OK, status);

  status = rados->conf_read_file(ctx->ceph_config_path.c_str());
  ARROW_RETURN_IF(status.code() != arrow::StatusCode::OK, status);

  status = rados->connect();
  ARROW_RETURN_IF(status.code() != arrow::StatusCode::OK, status);

  status = rados->ioctx_create(ctx->ceph_data_pool.c_str(), io_ctx.get());
  ARROW_RETURN_IF(status.code() != arrow::StatusCode::OK, status);

  return arrow::Status::OK();
}

void RadosConn::Shutdown() {
  if (connected) {
    rados->shutdown();
    connected = false;
  }
}

}  // namespace rados
}  // namespace skyhook
