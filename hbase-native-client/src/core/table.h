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

#include "connection.h"
#include "delete.h"
#include "exception.h"
#include "get.h"
#include "put.h"
#include "result.h"
#include "result_scanner.h"
#include "scan.h"
#include "rpc_client.h"
#include "table_name.h"
#include "pb_request_builder.h"

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>

class Connection;
class ResultScanner;

class Table {

 public:
  Table(TableName *tableName, Connection *connection);
  Table(const Table &);
  Table& operator=(const Table &);
  Result *get(const Get &get);
  std::vector<Result *> get(const std::vector<Get *>&);
  bool put(const Put &put);
  bool put(const std::vector<Put *>&);
  bool deleteRow(const Delete &deleteObj);
  bool deleteRow(const std::vector<Delete *> &);
  ResultScanner *getScanner(Scan & scan);
  ResultScanner* getScanner(const BYTE_ARRAY &family);
  ResultScanner* getScanner(const BYTE_ARRAY &family,
                            const BYTE_ARRAY &qualifier);
  void close();
  TableName *getName();

 private:
  TableName *tableName_;
  Connection *connection_;
  std::shared_ptr<RpcClient> rpcClient_;
  bool cleanupConnectionOnClose_;
  bool isClosed_;
  int client_retries_;
  //TODO:
  //Table Configuration as a new data member has to be introduced to get replica regions

  typedef struct {
    std::string tableRegionIP;
    int tableRsPort = 0;
  } RegionServerDetails;

  typedef struct {
    std::string tableRegionInfo;
    RegionServerDetails region_server_details;
  } RegionServerInfo;

  using GET_LIST_FOR_REGION = std::map<std::string, std::vector<Get *>>;
  using PUT_LIST_FOR_REGION = std::map<std::string, std::vector<Put *>>;
  using DELETE_LIST_FOR_REGION = std::map<std::string, std::vector<Delete *>>;
  using SOCKETINFO_FOR_REGION = std::map<size_t, RegionServerInfo *>;

  void GetCellsFromCellMetaBlock(std::vector<Cell> &cells,
                                 const CellScanner &cell_scanner);

  bool CheckExceptionAndRetryOrThrow(const HBaseException &exc,
                                     int &retry_count);

  RegionDetails* GetRegionDetails(const std::string &row_string);

  void Gets(SOCKETINFO_FOR_REGION &region_server_info_lookup,
            GET_LIST_FOR_REGION &regioninfo_rowinfo,
            std::map<std::string, hbase::pb::MultiRequest*> &multi_requests,
            std::vector<Result *> &list_results);

  void Puts(SOCKETINFO_FOR_REGION &region_server_info_lookup,
            PUT_LIST_FOR_REGION &regioninfo_rowinfo,
            std::map<std::string, hbase::pb::MultiRequest*> &multi_requests);

  void Deletes(SOCKETINFO_FOR_REGION &region_server_info_lookup,
               DELETE_LIST_FOR_REGION &regioninfo_rowinfo,
               std::map<std::string, hbase::pb::MultiRequest*> &multi_requests);

  void GetMultiResponse(
      std::map<std::string, hbase::pb::MultiRequest*> &multi_requests,
      SOCKETINFO_FOR_REGION &region_server_info_lookup,
      std::map<std::string, std::map<int, int>>&failed_result_or_exception_for_region_retries,
      std::vector<Result *> &list_results, const bool &fetch_result = false);

  void GetMultiResponseInParallel(
      const std::map<std::string, hbase::pb::MultiRequest*> &multi_requests,
      const SOCKETINFO_FOR_REGION &region_server_info_lookup,
      std::map<std::string, hbase::pb::MultiResponse *> &multi_responses,
      std::map<std::string, CellScanner *> &cell_list,
      std::map<std::string, int> &failed_requests);

  void CallMultiOps(
      const hbase::pb::MultiRequest &request,
      const SOCKETINFO_FOR_REGION &region_server_info_lookup,
      std::map<std::string, hbase::pb::MultiResponse*> &multi_responses,
      std::map<std::string, CellScanner *> &cell_list) const;

  void GetMultiRegionException(
      std::map<std::string, hbase::pb::MultiRequest*> &multi_requests,
      const std::string &region, const std::string &exception,
      std::map<std::string, int> &failed_requests,
      SOCKETINFO_FOR_REGION &region_server_info_lookup);

  Result *GetMultiResultOrException(
      const hbase::pb::RegionActionResult &region_action_result,
      const std::map<std::string, hbase::pb::MultiRequest*> &multi_requests,
      const CellScanner *cell_scanner,
      const std::string &region_value,
      std::map<std::string, std::map<int, int>>&failed_result_or_exception_for_region_retries);

  void GetMultiException(const std::string &exception, const bool &stack_trace,
                         const int &index, const std::string &region,
                         int &retry_count);

  bool CheckExceptionForRetries(const HBaseException &exc, int &retry_count);

  void ReleaseRegionServerInfo(
      SOCKETINFO_FOR_REGION &region_server_info_lookup);

};
