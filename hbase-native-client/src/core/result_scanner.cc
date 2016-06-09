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

#include "result_scanner.h"

#include <memory>
#include <queue>
#include <glog/logging.h>


#include "pb_request_builder.h"
#include "../rpc/generated/RPC.pb.h"
#include "utils.h"

ResultScanner::ResultScanner(Connection *connection, Scan *scan,
    TableName  *table_name) {
  connection_ = connection;
  scan_ = scan;
  region_details_ = nullptr;
  table_name_ = table_name;

  next_call_seq_ = 0;
  serverHasMoreResultsContext_ = false;
  serverHasMoreResults_ = false;
  heartbeatMessage_ = false;
  has_more_results_context_ = false;
  server_has_more_results_ = false;
  renew_ = false;
  scanner_id_ = -1L;
  region_details_ = connection_->FindTheRegionServerForTheTable(table_name_->GetName(),
      scan_->GetStartRowVal());

  client_retries_ = connection_->GetClientRetries();
  Open();
}

ResultScanner::~ResultScanner() {

}

Result *ResultScanner::Next() {

  if (0 == cache_.size()) {
    PopulateCache();
  }

  if (cache_.size() > 0) {
    Result *head = cache_.front();
    cache_.pop();
    return head;
  }

  return NULL;
}

void ResultScanner::ClearPartialResults() {
  partialresults_ = {};
  partialresultsRow_ = "";
}

void ResultScanner::AddToPartialResults(Result* res) {
  partialresultsRow_ = currentRow_;
  partialresults_.push(res);
}

void ResultScanner::AddResultsToCache(google::protobuf::Message *def_resp_type, CellScanner &cell_scanner) {

  hbase::pb::ScanResponse *resp =
      dynamic_cast<hbase::pb::ScanResponse *>(def_resp_type);
  int num_results = resp->cells_per_result_size();
  for (int i = 0; i < num_results; i++) {

    int num_cells = resp->cells_per_result(i);

    DLOG(INFO) << "noOfCells : " << num_cells;
    bool isPartial = resp->partial_flag_per_result_size() > i ?
        resp->partial_flag_per_result(i) : false;

    std::vector<Cell > cells;
    for (int j = 0; j < num_cells; j++) {
      if (cell_scanner.Advance()) {
        Cell *current_cell = cell_scanner.Current();
        //cells->push_back(current_cell);
        cells.push_back(*current_cell);
        delete current_cell;
      }
      else
      {
        //Throw IO Exception
      }
    }

    Result *pResult = new Result(cells, NULL, resp->has_stale(), resp->partial_flag_per_result(i));
    cache_.push(pResult);
    cells.clear();
  }
}

void ResultScanner::AddResultsToList(google::protobuf::Message *def_resp_type, CellScanner &cell_scanner) {

  hbase::pb::ScanResponse *resp =
      dynamic_cast<hbase::pb::ScanResponse *>(def_resp_type);
  int num_results = resp->cells_per_result_size();
  for (int i = 0; i < num_results; i++) {

    int num_cells = resp->cells_per_result(i);

    DLOG(INFO) << "noOfCells : " << num_cells;
    bool isPartial = resp->partial_flag_per_result_size() > i ?
        resp->partial_flag_per_result(i) : false;
    std::vector<Cell> cells;

    for (int j = 0; j < num_cells; j++) {
      if (cell_scanner.Advance()) {
        Cell *current_cell = cell_scanner.Current();
        //cells.push_back(*current_cell);
        cells.push_back(*current_cell);
        delete current_cell;
      }
      else
      {
        //Throw IO Exception
      }
    }
    Result *pResult = new Result(cells, NULL, resp->has_stale(), resp->partial_flag_per_result(i));
    if (isPartial) {
      isPartial_ = true;
      currentRow_ = cells.data()->Row();
      DLOG(INFO)<< "partial result detected :"<<partialresultsRow_<< std::endl;
    } else
      isPartial_ = false;

    resultsToAddToCache_.push_back(pResult);
    cells.clear();
  }
}

void ResultScanner::CreateCompleteRequest(int offset) {

  int loop =0;
  if(resultsToAddToCache_.size()>0)
    loop = resultsToAddToCache_.size()-offset;
  while(loop)
  {
    cache_.push(resultsToAddToCache_.front());
    resultsToAddToCache_.pop_front();

    loop--;
  }
}

void ResultScanner::GetResultsToAddToCache(google::protobuf::Message *def_resp_type,CellScanner &cell_scanner){

  hbase::pb::ScanResponse *resultsFromServer =
      dynamic_cast<hbase::pb::ScanResponse *>(def_resp_type);

  bool heartbeatMessage = resultsFromServer->heartbeat_message();
  int resultSize = resultsFromServer->cells_per_result_size();
  bool isBatchSet = scan_ != NULL && scan_->GetBatch();
  bool AllowPartials = scan_->GetAllowPartialResults();

  if (AllowPartials || isBatchSet) {
    AddResultsToCache(def_resp_type,cell_scanner);
    return;
  }

  AddResultsToList(def_resp_type,cell_scanner);
  int num_results = resultsFromServer->cells_per_result_size();
  //first partial result
  if (isPartial_ && partialresults_.empty()) {
    Result partial =  resultsToAddToCache_.back();
    AddToPartialResults(&partial);
    CreateCompleteRequest(1);
  }//remaining partial results
  else if (!partialresults_.empty()) {
    if (partialresultsRow_ == currentRow_) {
      if(!isPartial_) {
        CreateCompleteRequest(0);
        ClearPartialResults();
      }
    }//partial result for next row
    else {
      if(isPartial_) {
        Result result =  resultsToAddToCache_.back();
        AddToPartialResults(&result);
        CreateCompleteRequest(1);
      } else {
        CreateCompleteRequest(0);
        ClearPartialResults();
      }
    }
  }//not a partial result
  else
  {
    CreateCompleteRequest(0);
  }
}

void ResultScanner::PopulateCache() {

  bool is_retry;
  int retry_count = 0;
  do {
    try {
      is_retry = false;
      int number_of_rows = 100;
      int countdown = 100;

      bool done = false;
      while (!done) {
        std::unique_ptr<google::protobuf::Message> scanRequest(
            ProtoBufRequestBuilder::CreateScanRequestForScannerId(scanner_id_,
                number_of_rows, false, false, next_call_seq_, false));

        CellScanner cell_scanner;

        const std::string user_name(connection_->GetUser());
        const std::string service_name("ClientService");
        const std::string method_name("Scan");

        hbase::pb::ScanResponse response_type;
        google::protobuf::Message *def_resp_type = &response_type;

        std::shared_ptr<RpcClient> rpc_client_(connection_->GetRpcClient());
        std::unique_ptr<google::protobuf::Message> ptrResp(
            rpc_client_->Call(service_name, method_name, *scanRequest,
                region_details_->GetRegionServerIpAddr(),
                region_details_->GetRegionServerPort(), user_name,
                *def_resp_type, cell_scanner));
        def_resp_type = ptrResp.get();
        if (nullptr != def_resp_type) {
          next_call_seq_++;

          hbase::pb::ScanResponse *resp =
              dynamic_cast<hbase::pb::ScanResponse *>(def_resp_type);

          int num_results = resp->cells_per_result_size();
          GetResultsToAddToCache(def_resp_type,cell_scanner);
          done = true;


          if (resp->has_more_results() && !resp->more_results()) {
            //Close the scanner
            Close();
          }

          if (resp->has_heartbeat_message() &&
              resp->heartbeat_message() &&
              cache_.size() > 0) {
            return;
          }

          server_has_more_results_ = false;

          if (num_results > 0 && resp->has_more_results_in_region()) {
            server_has_more_results_ = true;
          }

          //TODO: deal with partial results
          //Partial results are dealt with

          if (server_has_more_results_) {
            return;
          }

          bool next_scanner = false;
          if (!server_has_more_results_) {
            if (!OpenNextRegion()) {
              close_scanner_=true;
              return;
            }
          }

        } else {
          DLOG(INFO) << "Nullptr in server response" << std::endl;
        }
      }
    }catch(const HBaseException &exc){

      is_retry = this->CheckExceptionAndRetryOrThrow(exc, retry_count);

    }
  }while(is_retry);
}

std::vector<Result*> *ResultScanner::Next(int num_rows) {
  return NULL;
}

bool ResultScanner::RenewLease() {
  return true;
}

void ResultScanner::Open() {

  DLOG(INFO) << "In " << __func__;
  bool is_retry;
  int retry_count = 0;
  do {
    try {
      is_retry = false;
      if (-1L != scanner_id_) {
        return;
      }

      std::unique_ptr<google::protobuf::Message> scanRequest(
          ProtoBufRequestBuilder::CreateScanRequest(region_details_->GetRegionName(),
              *scan_, 0, false));

      hbase::pb::ScanResponse response_type;
      google::protobuf::Message *def_resp_type = &response_type;

      CellScanner cell_scanner;

      const std::string user_name(connection_->GetUser());
      const std::string service_name("ClientService");
      const std::string method_name("Scan");

      std::shared_ptr<RpcClient> rpc_client_(connection_->GetRpcClient());
      std::unique_ptr<google::protobuf::Message> ptrResp(
          rpc_client_->Call(service_name, method_name, *scanRequest,
              region_details_->GetRegionServerIpAddr(),
              region_details_->GetRegionServerPort(), user_name,
              *def_resp_type, cell_scanner));

      def_resp_type = ptrResp.get();
      if (NULL != def_resp_type) {
        hbase::pb::ScanResponse *resp =
            dynamic_cast<hbase::pb::ScanResponse *>(def_resp_type);
        scanner_id_ = resp->scanner_id();
        next_call_seq_ = 0;
      }
    } catch(const HBaseException &exc) {

      is_retry = this->CheckExceptionAndRetryOrThrow(exc, retry_count);

    }
  } while(is_retry);
}

bool ResultScanner::CheckScanStopRow(const std::string &end_row_key) {
  if (scan_->GetStopRowVal().length() > 0) {
    std::string &stop_row_key = scan_->GetStopRowVal();
    if (end_row_key.compare(stop_row_key) >= 0) {
      return true;
    }
  }
  return false;
}

bool ResultScanner::OpenNextRegion() {
  Close();

  const std::string &end_key = region_details_->GetEndKey();
  if (0 == end_key.compare("") ||
      CheckScanStopRow(end_key)) {
    return false;
  }

  std::string lcl_end_key = end_key;

  region_details_ = connection_->FindTheRegionServerForTheTable(table_name_->GetName(),
      lcl_end_key);

  scan_->SetStartRowVal(lcl_end_key);
  Open();

  return true;
}


void ResultScanner::Close() {

  bool is_retry;
  int retry_count = 0;
  do {
    try {
      is_retry = false;
      if (-1L == scanner_id_) {
        return;
      }

      std::unique_ptr<google::protobuf::Message> scanRequest(
          ProtoBufRequestBuilder::CreateScanRequestForScannerId(scanner_id_,
              0, true, false));
      hbase::pb::ScanResponse response_type;
      google::protobuf::Message *def_resp_type = &response_type;

      CellScanner cell_scanner;

      const std::string user_name(connection_->GetUser());
      const std::string service_name("ClientService");
      const std::string method_name("Scan");

      std::shared_ptr<RpcClient> rpc_client_(connection_->GetRpcClient());
      std::unique_ptr<google::protobuf::Message> ptrResp(
          rpc_client_->Call(service_name, method_name, *scanRequest,
              region_details_->GetRegionServerIpAddr(),
              region_details_->GetRegionServerPort(), user_name,
              *def_resp_type, cell_scanner));

      scanner_id_ = -1L;
    } catch(const HBaseException &exc) {

      is_retry = this->CheckExceptionAndRetryOrThrow(exc, retry_count);

    }
  } while(is_retry);
}

bool ResultScanner::CheckExceptionAndRetryOrThrow(const HBaseException &exc, int &retry_count) {

  if (exc.RetryException()) {
    retry_count += 1;
    DLOG(INFO) << "retry_count[" << retry_count << "]; client_retries_[" << client_retries_ << "]";
    if (retry_count > client_retries_) {
      throw exc;
    } else {
      // Sleeping in microsecs to make sure we get the correct response after retries.
      usleep(2000*(retry_count));
      switch (exc.GetHBaseErrorCode()) {
      // Call Connection Invalidate Cache and retry again
      case HBaseException::HBASEERRCODES::ERR_REGION_MOVED:{
        DLOG(INFO) << "InvalidateRegionServerCache for " << table_name_->GetName() << " and checking again.";
        if(!this->connection_->InvalidateRegionServerCache(*table_name_)){
          DLOG(WARNING) << "Table Name [" << table_name_->GetName() << "] not found in cache. Cache still intact for it." ;
        }
        break;
      }
      default:
        break;
      }
    }
  } else {
    // All other exceptions will be thrown from here.
    throw exc;
  }
  return exc.RetryException();
}
