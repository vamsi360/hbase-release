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

#include "result.h"
#include "scan.h"
#include "region_details.h"
#include "connection.h"
#include "table_name.h"

#include <vector>
#include <queue>
#include <string>
#include "exception.h"

class Connection;

class ResultScanner {
 public:
  ResultScanner(Connection *connection, Scan *scan, TableName *table_name);
  virtual ~ResultScanner();
  Result *Next();
  std::vector<Result *> *Next(int num_rows);
  bool RenewLease();
  bool close_scanner_;

 private:
  void Open();
  bool OpenNextRegion();
  void Close();
  void PopulateCache();
  bool CheckScanStopRow(const std::string &end_row_key);
  void GetResultsToAddToCache( google::protobuf::Message *resultFromServer,CellScanner &cell_scanner);
  void ClearPartialResults();
  void CreateCompleteRequest(int offset);
  void AddToPartialResults(Result *res);
  void AddResultsToCache( google::protobuf::Message *resp, CellScanner &cell_scanner);
  void AddResultsToList(google::protobuf::Message *resp, CellScanner &cell_scanner);
  bool CheckExceptionAndRetryOrThrow(const HBaseException &exc, int &retry_count);
  std::queue <Result *> cache_;
  std::queue <Result *> partialresults_;
  std::deque <Result *> resultsToAddToCache_;
  std::string partialresultsRow_;
  std::string currentRow_;
  bool isPartial_;
  long scanner_id_;
  long next_call_seq_;
  bool serverHasMoreResultsContext_;
  bool serverHasMoreResults_;
  long scannerId_;
  bool heartbeatMessage_;
  bool renew_;
  bool closed_;
  bool has_more_results_context_;
  bool server_has_more_results_;
  Connection *connection_;
  Scan *scan_;
  RegionDetails *region_details_;
  TableName *table_name_;
  int client_retries_;
};
