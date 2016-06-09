/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#pragma once

#include <memory>
#include <google/protobuf/message.h>

#include "../rpc/generated/HBase.pb.h"
#include "../rpc/generated/Client.pb.h"
#include "../rpc/generated/Master.pb.h"
#include "../rpc/generated/Filter.pb.h"

#include "scan.h"
#include "get.h"
#include "put.h"
#include "delete.h"
#include "table_schema.h"
#include "key_value.h"

#include "util.h"

class ProtoBufRequestBuilder {

 public:
  ProtoBufRequestBuilder();
  virtual ~ProtoBufRequestBuilder();
  static std::unique_ptr<google::protobuf::Message> CreateScanRequest(
      const std::string &regionName, Scan & scan, int numberOfRows,
      bool closeScanner);
  static std::unique_ptr<google::protobuf::Message> CreateScanRequestForScannerId(
      long scannerId, int numberOfRows, bool closeScanner, bool trackMetrics);
  static std::unique_ptr<google::protobuf::Message> CreateScanRequestForScannerId(
      long scannerId, int numberOfRows, bool closeScanner, bool trackMetrics,
      long nextCallSeq, bool renew);
  static void CreateRegionSpecifier(const std::string &regionName,
                                    hbase::pb::RegionSpecifier &regObj);
  static void CreateMutationMessage(const Put &put,
                                    hbase::pb::MutationProto &mutate);
  static void CreateMutationMessage(const Delete &delete_obj,
                                    hbase::pb::MutationProto &mutate);
  static void CreateGetMessage(const Get &get, hbase::pb::Get &getObj);
  static hbase::pb::MutationProto_DeleteType ToDeleteType(
      const BYTE_TYPE &type);
  static void AddMutationFamilies(hbase::pb::MutationProto &mutate,
                                  const FAMILY_MAP &family_map);
  static std::unique_ptr<google::protobuf::Message> CreateGetRequest(
      const Get &get, const std::string &regionName);
  static std::map<std::string, hbase::pb::MultiRequest*> CreateGetRequest(
      const std::map<std::string, std::vector<Get *>> &regioninfo_rowinfo);
  static std::map<std::string, hbase::pb::MultiRequest*> CreateGetRequest(
      const std::map<std::string, std::vector<Get *>> &regioninfo_rowinfo,
      const std::map<std::string, std::map<int, int>> &failed_result_or_exception_retries,
      const int &threshold);

  static std::unique_ptr<google::protobuf::Message> CreatePutRequest(
      const Put &put, const std::string &regionName);
  static std::map<std::string, hbase::pb::MultiRequest*> CreatePutRequest(
      const std::map<std::string, std::vector<Put *>> &regioninfo_rowinfo);
  static std::map<std::string, hbase::pb::MultiRequest*> CreatePutRequest(
      const std::map<std::string, std::vector<Put *>> &regioninfo_rowinfo,
      const std::map<std::string, std::map<int, int>> &failed_result_or_exception_retries,
      const int &threshold);

  static std::unique_ptr<google::protobuf::Message> CreateDeleteRequest(
      const Delete &deleteObj, const std::string &regionName);
  static std::map<std::string, hbase::pb::MultiRequest*> CreateDeleteRequest(
      const std::map<std::string, std::vector<Delete *>> &regioninfo_rowinfo);
  static std::map<std::string, hbase::pb::MultiRequest*> CreateDeleteRequest(
      const std::map<std::string, std::vector<Delete *>> &regioninfo_rowinfo,
      const std::map<std::string, std::map<int, int>> &failed_result_or_exception_retries,
      const int &threshold);

  static void CreateTableName(const std::string &tableName,
                              hbase::pb::TableName &tableObj);
  static std::unique_ptr<google::protobuf::Message> CreateEnableTableRequest(
      const std::string &tableName);
  static std::unique_ptr<google::protobuf::Message> CreateDisableTableRequest(
      const std::string &tableName);
  static std::unique_ptr<google::protobuf::Message> CreateTableRequest(
      const TableSchema &table_schema);
  static std::unique_ptr<google::protobuf::Message> CreateDeleteTableRequest(
      const std::string &tableName);
  static std::unique_ptr<google::protobuf::Message> CreateListTablesRequest(
      const std::string &regEx, const bool &includeSysTables);

  static std::unique_ptr<google::protobuf::Message> CreateProcedureRequest(
      const int & procedureId);

};
