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

#include "table.h"

#include <memory>
#include <sstream>
#include <thread>
#include <mutex>

#include <Poco/Hash.h>
#include <Poco/ByteOrder.h>
#include <glog/logging.h>

#include "utils.h"

std::mutex multiops_mutex;

Table::Table(TableName *tableName, Connection *connection)
    : tableName_(tableName),
      connection_(connection),
      cleanupConnectionOnClose_(false),
      isClosed_(false) {
  rpcClient_ = connection_->GetRpcClient();
  client_retries_ = connection_->GetClientRetries();
}

Table::Table(const Table &table) {

  DLOG(INFO) << "Copy";
  this->tableName_ = table.tableName_;
}

Table& Table::operator=(const Table &table) {

  DLOG(INFO) << "Assignment";
  return *this;

}

Result *Table::get(const Get &get) {

  Result *result = nullptr;

  if (CONSISTENCY::STRONG == get.GetConsistency()) {
    bool is_retry;
    int retry_count = 0;
    do {
      try {
        is_retry = false;
        std::vector < Result > cells;
        RegionDetails* region_details = nullptr;
        region_details = GetRegionDetails(get.GetRowAsString());
        std::string tableRegionInfo("");
        std::string tableRegionIP;
        int tableRsPort = 0;
        tableRegionInfo = region_details->GetRegionName();
        tableRegionIP = region_details->GetRegionServerIpAddr();
        tableRsPort = region_details->GetRegionServerPort();

        std::unique_ptr < google::protobuf::Message
            > req(
                ProtoBufRequestBuilder::CreateGetRequest(get, tableRegionInfo));

        hbase::pb::GetResponse response_type;
        google::protobuf::Message *def_resp_type = &response_type;
        CellScanner cell_scanner;

        const std::string user_name(connection_->GetUser());
        const std::string service_name("ClientService");
        const std::string method_name("Get");

        std::unique_ptr < google::protobuf::Message
            > ptrResp(
                rpcClient_->Call(service_name, method_name, *req, tableRegionIP,
                                 tableRsPort, user_name, *def_resp_type,
                                 cell_scanner));

        def_resp_type = ptrResp.get();
        if (nullptr != def_resp_type) {

          std::vector < Cell > cells;
          bool pb_result_exists = false;
          bool pb_result_stale = false;
          bool pb_result_partial = false;

          hbase::pb::GetResponse *resp =
              dynamic_cast<hbase::pb::GetResponse *>(def_resp_type);
          hbase::pb::Result resultObj = resp->result();
          DLOG(INFO) << "[" << __LINE__ << ":" << __func__
              << "] DBG: Response:[" << resp->ByteSize() << "]: "
              << resp->DebugString();
          DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: "
              << resultObj.DebugString() << "; Cell Size: "
              << resultObj.cell_size();

          if (resultObj.has_exists()) {
            if ((resultObj.cell_size() > 0)
                || (resultObj.has_associated_cell_count()
                    && resultObj.associated_cell_count() > 0)) {
              std::stringstream error_str("");
              error_str << "Bad proto: exists with cells is not allowed:\n "
                  << resultObj.DebugString();
              throw HBaseException(error_str.str());
            }

            if (resultObj.stale()) {
              pb_result_stale = true;
              pb_result_exists = (resultObj.exists()) ? true : false;
            } else {
              pb_result_stale = false;
              pb_result_exists = (resultObj.exists()) ? true : false;
            }
            result = new Result(cells, pb_result_exists, pb_result_stale,
                                pb_result_partial);
            return result;
          } else {
            pb_result_exists = false;
          }
          pb_result_stale =
              ((resultObj.has_stale()) && (resultObj.stale())) ? true : false;
          pb_result_partial =
              ((resultObj.has_partial()) && (resultObj.partial())) ?
                  true : false;
          int cellSize = resultObj.cell_size();

          this->GetCellsFromCellMetaBlock(cells, cell_scanner);

          for (int var = 0; var < cellSize; ++var) {
            const hbase::pb::Cell &cell = resultObj.cell(var);

            std::unique_ptr < Cell
                > cell_ptr(
                    new Cell(
                        cell.row(),
                        cell.family(),
                        cell.qualifier(),
                        cell.timestamp(),
                        cell.value(),
                        cell.tags(),
                        cell.has_cell_type() ?
                            static_cast<CellType::cellType>(cell.cell_type()) :
                            CellType::UNKNOWN));
            if (cell_ptr.get())
              cells.push_back(*cell_ptr.get());
          }

          result = new Result(cells, pb_result_exists, pb_result_stale,
                              pb_result_partial);

        }

      } catch (const HBaseException &exc) {

        is_retry = this->CheckExceptionAndRetryOrThrow(exc, retry_count);

      }

    } while (is_retry);
  } else {
    //TODO FOR READ REPLICAS
    // Get Client Retries number
    // getPrimaryCallTimeoutMicroSecond
    // Result call
    int replica_id = 0;  //DEFAULT_REPLICA_ID
    bool is_targetreplica_specified =
        (get.GetReplicaId() >= 0) ?
            (replica_id = get.GetReplicaId()) : (replica_id = 0);
    LOG(INFO) << "replica_id:- " << replica_id
        << "; is_targetreplica_specified:- " << is_targetreplica_specified;
    RegionDetails* region_details = nullptr;
    const std::vector<REGION_SERVER_DETAILS> &replica_regions = region_details
        ->GetReplicaRegionServers();
    for (const auto region : replica_regions) {
      LOG(INFO) << "REPLICA RS[" << region.first << ":" << region.second << "]";
    }
    if (is_targetreplica_specified) {
      //result =
      //addCallsForReplica(cs, rl, get.getReplicaId(), get.getReplicaId());
    } else {
      //addCallsForReplica(cs, rl, 0, 0);
      try {

      } catch (...) {

      }
      // submit call for the all of the secondaries at once
      // addCallsForReplica(cs, rl, 1, rl.size() - 1);
    }
  }
  return result;
}

std::vector<Result *> Table::get(const std::vector<Get *> &gets) {

  std::vector<Result *> list_results;
  GET_LIST_FOR_REGION regioninfo_rowinfo;
  SOCKETINFO_FOR_REGION region_server_info_lookup;
  for (const auto get : gets) {
    if (nullptr == get)
      continue;
    RegionDetails *region_details = GetRegionDetails(get->GetRowAsString());
    RegionServerInfo *region_server_info = new RegionServerInfo;
    region_server_info->tableRegionInfo = region_details->GetRegionName();
    region_server_info->region_server_details.tableRegionIP = region_details
        ->GetRegionServerIpAddr();
    region_server_info->region_server_details.tableRsPort = region_details
        ->GetRegionServerPort();
    regioninfo_rowinfo[region_server_info->tableRegionInfo].push_back(get);
    region_server_info_lookup[Poco::hash(region_server_info->tableRegionInfo)] =
        region_server_info;
  }

  std::map<std::string, hbase::pb::MultiRequest*> multi_requests =
      ProtoBufRequestBuilder::CreateGetRequest(regioninfo_rowinfo);

  this->Gets(region_server_info_lookup, regioninfo_rowinfo, multi_requests,
             list_results);

  this->ReleaseRegionServerInfo(region_server_info_lookup);

  for (auto &region_dml : regioninfo_rowinfo) {
    regioninfo_rowinfo[region_dml.first].clear();
    regioninfo_rowinfo.erase(region_dml.first);
  }

  LOG(INFO) << "list_results size:- " << list_results.size();
  return list_results;
}

bool Table::put(const Put &put) {

  bool return_status = false;
  bool is_retry;
  int retry_count = 0;
  do {
    try {
      is_retry = false;
      std::string row = put.GetRowAsString();
      std::string tableRegionInfo("");
      std::string tableRegionIP;
      int tableRsPort = 0;
      int counter = 1;

      while (tableRegionInfo == "") {

        connection_->FindTheRegionServerForTheTable(tableName_, row,
                                                    tableRegionInfo,
                                                    tableRegionIP, tableRsPort);
        if (tableRegionInfo == "")
          usleep(2000 * (counter += 1));
      }

      std::unique_ptr < google::protobuf::Message
          > putReq(
              ProtoBufRequestBuilder::CreatePutRequest(put, tableRegionInfo));

      hbase::pb::MultiResponse response_type;
      google::protobuf::Message *def_resp_type = &response_type;
      CellScanner cell_scanner;

      const std::string user_name(connection_->GetUser());
      const std::string service_name("ClientService");
      const std::string method_name("Multi");

      std::unique_ptr < google::protobuf::Message
          > ptrResp(
              rpcClient_->Call(service_name, method_name, *putReq,
                               tableRegionIP, tableRsPort, user_name,
                               *def_resp_type, cell_scanner));

      def_resp_type = ptrResp.get();
      if (nullptr != def_resp_type) {
        hbase::pb::MultiResponse *resp =
            dynamic_cast<hbase::pb::MultiResponse *>(def_resp_type);
        DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Response:["
            << resp->ByteSize() << "]: " << resp->DebugString();
        if (resp->has_processed())
          DLOG(INFO) << "[" << __LINE__ << ":" << __func__
              << "] DBG: Put processed[" << resp->processed() << "]";

        for (int num = 0; num < resp->regionactionresult_size(); num++) {

          hbase::pb::RegionActionResult region_action_result = resp
              ->regionactionresult(num);
          if (region_action_result.has_exception()) {
            throw HBaseException(region_action_result.exception().value(),
                                 true);
          }

          for (int result_num = 0;
              result_num < region_action_result.resultorexception_size();
              result_num++) {

            hbase::pb::ResultOrException result_or_exception =
                region_action_result.resultorexception(result_num);
            if (result_or_exception.has_exception()) {
              throw HBaseException(result_or_exception.exception().value(),
                                   true);
            } else {
              return_status = true;
            }
          }
        }
      }

    } catch (const HBaseException &exc) {

      is_retry = this->CheckExceptionAndRetryOrThrow(exc, retry_count);

    }

  } while (is_retry);
  return return_status;
}

bool Table::put(const std::vector<Put *> &puts) {

  std::map<std::string, std::vector<Put *>> regioninfo_rowinfo;
  SOCKETINFO_FOR_REGION region_server_info_lookup;

  for (auto &put : puts) {
    put->Display();
    RegionDetails *region_details = nullptr;
    region_details = GetRegionDetails(put->GetRowAsString());
    RegionServerInfo *region_server_info = new RegionServerInfo;

    region_server_info->tableRegionInfo = region_details->GetRegionName();
    region_server_info->region_server_details.tableRegionIP = region_details
        ->GetRegionServerIpAddr();
    region_server_info->region_server_details.tableRsPort = region_details
        ->GetRegionServerPort();
    delete region_details;
    regioninfo_rowinfo[region_server_info->tableRegionInfo].push_back(put);
    region_server_info_lookup[Poco::hash(region_server_info->tableRegionInfo)] =
        region_server_info;

  }

  std::map<std::string, hbase::pb::MultiRequest*> multi_requests =
      ProtoBufRequestBuilder::CreatePutRequest(regioninfo_rowinfo);

  this->Puts(region_server_info_lookup, regioninfo_rowinfo, multi_requests);

  this->ReleaseRegionServerInfo(region_server_info_lookup);

  for (auto &region_dml : regioninfo_rowinfo) {
    regioninfo_rowinfo[region_dml.first].clear();
    regioninfo_rowinfo.erase(region_dml.first);
  }

  return true;
}

bool Table::deleteRow(const Delete &deleteObj) {

  bool return_status = false;
  bool is_retry;
  int retry_count = 0;
  do {
    try {
      is_retry = false;
      std::string row = deleteObj.GetRowAsString();
      std::string tableRegionInfo;
      std::string tableRegionIP;
      int tableRsPort = 0;
      int counter = 1;
      while (tableRegionInfo == "") {

        connection_->FindTheRegionServerForTheTable(tableName_, row,
                                                    tableRegionInfo,
                                                    tableRegionIP, tableRsPort);
        usleep(2000 * (counter += 1));
      }

      std::unique_ptr < google::protobuf::Message
          > deleteReq(
              std::move(
                  ProtoBufRequestBuilder::CreateDeleteRequest(
                      deleteObj, tableRegionInfo)));
      DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Response:["
          << deleteReq->ByteSize() << "]: " << deleteReq->DebugString();

      hbase::pb::MutateResponse response_type;
      google::protobuf::Message *def_resp_type = &response_type;
      CellScanner cell_scanner;

      const std::string user_name(connection_->GetUser());
      const std::string service_name("ClientService");
      const std::string method_name("Mutate");

      std::unique_ptr < google::protobuf::Message
          > ptrResp(
              rpcClient_->Call(service_name, method_name, *deleteReq,
                               tableRegionIP, tableRsPort, user_name,
                               *def_resp_type, cell_scanner));
      def_resp_type = ptrResp.get();
      if (nullptr != def_resp_type) {
        hbase::pb::MutateResponse *mutateResp =
            dynamic_cast<hbase::pb::MutateResponse *>(def_resp_type);
        DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Response:["
            << mutateResp->ByteSize() << "]: " << mutateResp->DebugString();
        if (mutateResp->has_processed())
          DLOG(INFO) << "[" << __LINE__ << ":" << __func__
              << "] DBG: Delete Processed:" << mutateResp->processed();

        return_status = true;
      }

    } catch (const HBaseException &exc) {

      is_retry = this->CheckExceptionAndRetryOrThrow(exc, retry_count);

    }

  } while (is_retry);
  return return_status;
}

bool Table::deleteRow(const std::vector<Delete *> &deletes) {

  std::map<std::string, std::vector<Delete *>> regioninfo_rowinfo;
  SOCKETINFO_FOR_REGION region_server_info_lookup;
  for (const auto delete_req : deletes) {

    delete_req->Display();
    RegionDetails* region_details = nullptr;
    region_details = GetRegionDetails(delete_req->GetRowAsString());
    RegionServerInfo *region_server_info = new RegionServerInfo;

    region_server_info->tableRegionInfo = region_details->GetRegionName();
    region_server_info->region_server_details.tableRegionIP = region_details
        ->GetRegionServerIpAddr();
    region_server_info->region_server_details.tableRsPort = region_details
        ->GetRegionServerPort();

    regioninfo_rowinfo[region_server_info->tableRegionInfo].push_back(
        delete_req);
    region_server_info_lookup[Poco::hash(region_server_info->tableRegionInfo)] =
        region_server_info;

  }

  std::map<std::string, hbase::pb::MultiRequest*> multi_requests =
      ProtoBufRequestBuilder::CreateDeleteRequest(regioninfo_rowinfo);

  this->Deletes(region_server_info_lookup, regioninfo_rowinfo, multi_requests);

  this->ReleaseRegionServerInfo(region_server_info_lookup);

  for (auto &region_dml : regioninfo_rowinfo) {
    regioninfo_rowinfo[region_dml.first].clear();
    regioninfo_rowinfo.erase(region_dml.first);
  }

  return true;

}

void Table::close() {
  if (cleanupConnectionOnClose_)
    if (nullptr != connection_)
      connection_->Close();
  isClosed_ = true;
  return;
}

TableName * Table::getName() {
  if (nullptr == this->tableName_)
    return nullptr;
  else
    return this->tableName_;
}

bool Table::CheckExceptionAndRetryOrThrow(const HBaseException &exc,
                                          int &retry_count) {

  if (exc.RetryException()) {
    retry_count += 1;
    DLOG(INFO) << "retry_count[" << retry_count << "]; client_retries_["
        << client_retries_ << "]";
    if (retry_count > client_retries_) {
      throw exc;
    } else {
      // Sleeping in microsecs to make sure we get the correct response after retries.
      usleep(2000 * (retry_count));
      switch (exc.GetHBaseErrorCode()) {
        // Call Connection Invalidate Cache and retry again
        case HBaseException::HBASEERRCODES::ERR_REGION_MOVED: {
          DLOG(INFO) << "InvalidateRegionServerCache for "
              << tableName_->GetName() << " and checking again.";
          if (!this->connection_->InvalidateRegionServerCache(*tableName_)) {
            DLOG(WARNING) << "Table Name [" << tableName_->GetName()
                << "] not found in cache. Cache still intact for it.";
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

bool Table::CheckExceptionForRetries(const HBaseException &exc,
                                     int &retry_count) {

  bool retry = false;
  if (exc.RetryException()) {
    retry_count += 1;
    retry = true;
  }
  DLOG(INFO) << "retry_count[" << retry_count << "]; client_retries_["
      << client_retries_ << "]";
  return retry;
}

void Table::GetCellsFromCellMetaBlock(std::vector<Cell> &cells,
                                      const CellScanner &cell_scanner) {

  const char *cell_block = cell_scanner.GetData();
  int cell_size = cell_scanner.GetDataLength();

  int offset = 0;
  unsigned int cell_size_length;

  const char *pCurrent = cell_block;
  if (cell_size > 0) {

    while (cell_size != offset) {

      unsigned int *pSize = (unsigned int*) pCurrent;
      cell_size_length = *pSize;
      CommonUtils::SwapByteOrder(cell_size_length);
      pCurrent += Bytes::SIZEOF_INT;
      offset += Bytes::SIZEOF_INT;

      BYTE_ARRAY bytes;
      bytes.insert(bytes.end(), pCurrent, pCurrent + cell_size_length);
      Cell *cell = Cell::CreateCell(bytes);

      pCurrent += cell->GetKeyValue()->GetKeyValueLength();
      offset += cell->GetKeyValue()->GetKeyValueLength();
      cells.push_back(*cell);
      delete cell;
    }

  }
}

ResultScanner *Table::getScanner(Scan &scan) {
  ResultScanner *resScanner = new ResultScanner(connection_, &scan, tableName_);
  return resScanner;
}

ResultScanner *Table::getScanner(const BYTE_ARRAY &family) {

  Scan scan;
  scan.AddFamily(family);
  ResultScanner *scanner = this->getScanner(scan);
  return scanner;

}

ResultScanner *Table::getScanner(const BYTE_ARRAY &family,
                                 const BYTE_ARRAY &qualifier) {

  Scan scan;
  scan.AddColumn(family, qualifier);
  ResultScanner *scanner = this->getScanner(scan);
  return scanner;
}

RegionDetails* Table::GetRegionDetails(const std::string &row_string) {

  RegionDetails *region_details = nullptr;

  int counter = 0;
  while (counter != this->client_retries_) {

    counter += 1;
    region_details = connection_->FindTheRegionServerForTheTable(tableName_,
                                                                 row_string);
    if (nullptr != region_details) {
      if (0 == region_details->GetRegionName().size()) {
        delete region_details;
        region_details = nullptr;
        usleep(2000 * counter);
      } else {
        break;
      }
    } else
      usleep(2000 * counter);
  }

  return region_details;
}

void Table::CallMultiOps(
    const hbase::pb::MultiRequest &request,
    const SOCKETINFO_FOR_REGION &region_server_info_lookup,
    std::map<std::string, hbase::pb::MultiResponse *> &multi_responses,
    std::map<std::string, CellScanner *> &cell_list) const {

  std::lock_guard < std::mutex > guard(multiops_mutex);

  size_t region_hash = Poco::hash(request.regionaction(0).region().value());
  std::string ip_address(
      region_server_info_lookup.at(region_hash)->region_server_details
          .tableRegionIP);
  int port(
      region_server_info_lookup.at(region_hash)->region_server_details
          .tableRsPort);

  DLOG(INFO) << request.DebugString();
  DLOG(INFO) << "[" << this << ":" << request.regionaction_size() << "]; ["
      << ip_address << ":" << port << "]";
  hbase::pb::MultiResponse response_type;
  google::protobuf::Message *def_resp_type = &response_type;
  CellScanner *cell_scanner = new CellScanner();
  const std::string user_name(this->connection_->GetUser());
  const std::string service_name("ClientService");
  const std::string method_name("Multi");

  std::unique_ptr < google::protobuf::Message
      > ptrResp(
          this->rpcClient_->Call(service_name, method_name, request, ip_address,
                                 port, user_name, *def_resp_type,
                                 *cell_scanner));

  def_resp_type = ptrResp.get();

  if (nullptr != def_resp_type) {

    hbase::pb::MultiResponse *multi_resp_tmp =
        dynamic_cast<hbase::pb::MultiResponse *>(def_resp_type);
    hbase::pb::MultiResponse *multi_resp = new hbase::pb::MultiResponse();
    *multi_resp = *multi_resp_tmp;
    DLOG(INFO) << multi_resp->DebugString();
    multi_responses.insert(
        std::pair<const std::string, hbase::pb::MultiResponse*>(
            request.regionaction(0).region().value(), multi_resp));
    cell_list.insert(
        std::pair<const std::string, CellScanner*>(
            request.regionaction(0).region().value(), cell_scanner));
  } else {
    multi_responses.insert(
        std::pair<const std::string, hbase::pb::MultiResponse*>(
            request.regionaction(0).region().value(), nullptr));
    cell_list.insert(
        std::pair<const std::string, CellScanner*>(
            request.regionaction(0).region().value(), nullptr));
  }
}

void Table::GetMultiResponseInParallel(
    const std::map<std::string, hbase::pb::MultiRequest*> &multi_requests,
    const SOCKETINFO_FOR_REGION &region_server_info_lookup,
    std::map<std::string, hbase::pb::MultiResponse *> &multi_responses,
    std::map<std::string, CellScanner *> &cell_list,
    std::map<std::string, int> &failed_requests) {

  std::vector < std::thread > multiops_threads;
  unsigned int num_threads = std::thread::hardware_concurrency();
  DLOG(INFO) << "Num of threads supported " << num_threads;
  DLOG(INFO) << "multi_requests.size() " << multi_requests.size();
  DLOG(INFO) << "Table::*this" << this;

  unsigned int index = 0;
  auto itr = multi_requests.begin();
  while (index < multi_requests.size()) {
    DLOG(INFO) << index << "< " << multi_requests.size();
    multiops_threads.clear();

    for (;
        (multiops_threads.size() != num_threads)
            && (itr != multi_requests.end()); ++itr, ++index) {
      DLOG(INFO) << multiops_threads.size() << "!" << num_threads;
      std::string region_value = itr->second->regionaction(0).region().value();
      bool call_multiops = false;
      if (failed_requests.size() > 0) {
        DLOG(INFO) << " in failure retry map";
        if (failed_requests.count(region_value) > 0) {
          if (failed_requests[region_value] <= this->client_retries_) {
            DLOG(INFO) << "Found " << region_value << " in failure retry map";
            call_multiops = true;
          } else {

            DLOG(INFO)
                << "max retries reached for multi request. We will erase this value";
            DLOG(INFO) << "failed_requests.size " << failed_requests.size();
            failed_requests.erase(region_value);
            DLOG(INFO) << "failed_requests.size " << failed_requests.size();
          }
        }
      } else {
        call_multiops = true;
      }
      if (call_multiops) {
        multiops_threads.push_back(
            std::thread(&Table::CallMultiOps, this, std::ref(*itr->second),
                        std::ref(region_server_info_lookup),
                        std::ref(multi_responses), std::ref(cell_list)));

      }
      DLOG(INFO) << "multiops_threads.ssize()::" << multiops_threads.size();
    }
    for (auto& thread : multiops_threads) {
      thread.join();
    }
  }

}

void Table::ReleaseRegionServerInfo(
    SOCKETINFO_FOR_REGION &region_server_info_lookup) {
  for (auto &ptr : region_server_info_lookup) {
    if (nullptr != ptr.second) {
      delete ptr.second;
      ptr.second = nullptr;
    }
    region_server_info_lookup.erase(ptr.first);
  }
}

void Table::Gets(
    SOCKETINFO_FOR_REGION&region_server_info_lookup,
    GET_LIST_FOR_REGION &regioninfo_rowinfo,
    std::map<std::string, hbase::pb::MultiRequest*> &multi_requests,
    std::vector<Result *> &list_results) {

  std::map<std::string, std::map<int, int>> failed_result_or_exception_retries;

  while (multi_requests.size() != 0) {

    DLOG(INFO) << "multi_requests.size()" << multi_requests.size();

    this->GetMultiResponse(multi_requests, region_server_info_lookup,
                           failed_result_or_exception_retries, list_results,
                           true);
    multi_requests = ProtoBufRequestBuilder::CreateGetRequest(
        regioninfo_rowinfo, failed_result_or_exception_retries,
        this->client_retries_);
  }
}

void Table::Puts(
    SOCKETINFO_FOR_REGION& region_server_info_lookup,
    std::map<std::string, std::vector<Put *>> &regioninfo_rowinfo,
    std::map<std::string, hbase::pb::MultiRequest*> &multi_requests) {

  std::map<std::string, std::map<int, int>> failed_result_or_exception_retries;
  std::vector<Result *> list_results;

  this->GetMultiResponse(multi_requests, region_server_info_lookup,
                         failed_result_or_exception_retries, list_results);

}

void Table::Deletes(
    SOCKETINFO_FOR_REGION& region_server_info_lookup,
    std::map<std::string, std::vector<Delete *>> &regioninfo_rowinfo,
    std::map<std::string, hbase::pb::MultiRequest*> &multi_requests) {

  std::map<std::string, std::map<int, int>> failed_result_or_exception_retries;
  std::vector<Result *> list_results;

  DLOG(INFO) << "multi_requests.size()" << multi_requests.size();

  this->GetMultiResponse(multi_requests, region_server_info_lookup,
                         failed_result_or_exception_retries, list_results);

}

void Table::GetMultiResponse(
    std::map<std::string, hbase::pb::MultiRequest*> &multi_requests,
    SOCKETINFO_FOR_REGION &region_server_info_lookup,
    std::map<std::string, std::map<int, int>>& failed_result_or_exception_retries,
    std::vector<Result *> &list_results, const bool &fetch_result) {

  std::map<std::string, int> failed_requests;
  bool retry_multi_request = false;

  do {

    bool any_failed_request = false;
    std::map<std::string, CellScanner *> cell_list;
    std::map<std::string, hbase::pb::MultiResponse *> multi_responses;

    this->GetMultiResponseInParallel(multi_requests, region_server_info_lookup,
                                     multi_responses, cell_list,
                                     failed_requests);
    DLOG(INFO) << "multi_responses.size()" << multi_responses.size();
    for (const auto response : multi_responses) {
      if (!any_failed_request)
        any_failed_request = false;
      if (nullptr != response.second) {
        DLOG(INFO) << "Parsing response for " << response.first;
        const auto resp = response.second;
        if (failed_requests.size() > 0)
          DLOG(INFO) << resp->DebugString();
        if (resp->has_processed())
          DLOG(INFO) << "[" << __LINE__ << ":" << __func__
              << "] DBG: Multi processed[" << resp->processed() << "]"
              << std::endl;
        for (int num = 0; num < resp->regionactionresult_size(); num++) {
          hbase::pb::RegionActionResult region_action_result = resp
              ->regionactionresult(num);
          if (region_action_result.has_exception()) {
            DLOG(INFO) << "Received an exception for region " << response.first
                << ". We need to push back this and see what to do if retry or not";
            DLOG(INFO) << "Exception is "
                << region_action_result.exception().name() << ": "
                << region_action_result.exception().value();
            DLOG(INFO) << "Exception Received for "
                << multi_requests[response.first]->regionaction(0).region()
                    .value();
            std::unique_ptr < HBaseException
                > exc(
                    new HBaseException(region_action_result.exception().value(),
                                       true));
            if (exc.get()) {
              any_failed_request = this->CheckExceptionForRetries(
                  *exc.get(),
                  failed_requests[multi_requests[response.first]->regionaction(
                      0).region().value()]);
              if (any_failed_request) {
                std::string row_string("");
                switch (exc.get()->GetHBaseErrorCode()) {
                  case HBaseException::HBASEERRCODES::ERR_REGION_MOVED: {
                    // Call Connection Invalidate Cache and retry again
                    this->connection_->InvalidateRegionServerCache(
                        *this->tableName_);
                    if (multi_requests[response.first]->regionaction(0)
                        .action_size() > 0) {
                      if (multi_requests[response.first]->regionaction(0).action(
                          0).has_get()) {
                        row_string = multi_requests[response.first]
                            ->regionaction(0).action(0).get().row();
                      }
                      if (multi_requests[response.first]->regionaction(0).action(
                          0).has_mutation()) {
                        row_string = multi_requests[response.first]
                            ->regionaction(0).action(0).mutation().row();
                      }
                    }
                    break;
                  }
                  default:
                    break;
                }
                if (row_string.size() > 0) {
                  RegionDetails *region_details = nullptr;
                  region_details = GetRegionDetails(row_string);
                  DLOG(INFO) << "Deleting ["
                      << region_server_info_lookup[Poco::hash(response.first)]
                          ->region_server_details.tableRegionIP << ":"
                      << region_server_info_lookup[Poco::hash(response.first)]
                          ->region_server_details.tableRsPort << "]";
                  auto &region_details_ptr = region_server_info_lookup[
                      Poco::hash(response.first)];
                  delete region_details_ptr;
                  region_details_ptr = nullptr;
                  DLOG(INFO) << "Value is null after delete";
                  RegionServerInfo *region_server_info = new RegionServerInfo;
                  region_server_info->tableRegionInfo = region_details
                      ->GetRegionName();
                  region_server_info->region_server_details.tableRegionIP =
                      region_details->GetRegionServerIpAddr();
                  region_server_info->region_server_details.tableRsPort =
                      region_details->GetRegionServerPort();
                  region_server_info_lookup[Poco::hash(
                      region_server_info->tableRegionInfo)] =
                      region_server_info;
                  DLOG(INFO) << "Set the value ["
                      << region_server_info_lookup[Poco::hash(
                          region_server_info->tableRegionInfo)]
                          ->region_server_details.tableRegionIP << ":"
                      << region_server_info_lookup[Poco::hash(
                          region_server_info->tableRegionInfo)]
                          ->region_server_details.tableRsPort << "]";

                } else {
                  DLOG(INFO)
                      << "Encountered Region moved exception but can't get the correction region in region map";
                }
              }
              continue;
            }
          } else {
            // In case of success result retries will be > num_of_retries to indicate success
            failed_requests[multi_requests[response.first]->regionaction(0)
                .region().value()] = this->client_retries_ + 1;
          }
          if (fetch_result) {
            DLOG(INFO) << "response.first [" << response.first << "]";
            DLOG(INFO)
                << "multi_requests.at(response.first)->regionaction(0).region().value() ["
                << multi_requests.at(response.first)->regionaction(0).region()
                    .value() << "]";
            DLOG(INFO)
                << "failed_result_or_exception_retries[response.first].size() ["
                << failed_result_or_exception_retries[response.first].size()
                << "]";
            Result *result = this->GetMultiResultOrException(
                region_action_result, multi_requests, cell_list[response.first],
                response.first, failed_result_or_exception_retries);
            if (nullptr != result)
              list_results.push_back(result);
          }
        }
      } else {
        DLOG(INFO) << "Resp is nullptr";
      }
    }

    retry_multi_request = (failed_requests.size() > 0) ? true : false;
    DLOG(INFO) << "retry_multi_request [" << retry_multi_request
        << "]; retry_multi_request.size() [" << failed_requests.size() << "];";

  } while (failed_requests.size() != 0);

}

Result* Table::GetMultiResultOrException(
    const hbase::pb::RegionActionResult &region_action_result,
    const std::map<std::string, hbase::pb::MultiRequest*> &multi_requests,
    const CellScanner *cell_scanner,
    const std::string &region_value,
    std::map<std::string, std::map<int, int>> &failed_result_or_exception_retries) {
  Result *result = nullptr;
  std::vector < Cell > cells;
  bool pb_result_stale = false;
  bool pb_result_exists = false;
  bool pb_result_partial = false;
  DLOG(INFO) << "\n" << region_action_result.DebugString();
  for (int result_num = 0;
      result_num < region_action_result.resultorexception_size();
      result_num++) {
    hbase::pb::ResultOrException result_or_exception = region_action_result
        .resultorexception(result_num);

    if (result_or_exception.has_exception()) {
      std::string exception(result_or_exception.exception().value());
      this->GetMultiException(
          exception,
          true,
          result_or_exception.index(),
          multi_requests.at(region_value)->regionaction(0).region().value(),
          failed_result_or_exception_retries[region_value][result_or_exception
              .index()]);
      continue;
    }

    if (result_or_exception.has_result()) {
      // In case of success result retries will be > num_of_retries to indicate success
      failed_result_or_exception_retries[region_value][result_or_exception.index()] =
          this->client_retries_ + 1;
      DLOG(INFO) << "We have some result for " << result_or_exception.index()
          << "; " << result_or_exception.result().cell_size() << "; "
          << result_or_exception.DebugString();
      hbase::pb::Result pb_result = result_or_exception.result();

      if (pb_result.has_exists()) {
        if ((pb_result.cell_size() > 0)
            || (pb_result.has_associated_cell_count()
                && pb_result.associated_cell_count() > 0)) {
          std::stringstream error_str("");
          error_str << "Bad proto: exists with cells is not allowed:["
              << pb_result.DebugString() << "]";

          this->GetMultiException(
              error_str.str(),
              false,
              result_or_exception.index(),
              multi_requests.at(region_value)->regionaction(0).region().value(),
              failed_result_or_exception_retries[region_value][result_or_exception
                  .index()]);
          continue;
        }
        if (pb_result.stale()) {
          pb_result_stale = true;
          pb_result_exists = (pb_result.exists()) ? true : false;
        } else {
          pb_result_stale = false;
          pb_result_exists = (pb_result.exists()) ? true : false;
        }
      } else {
        pb_result_exists = false;
      }
      pb_result_stale =
          ((pb_result.has_stale()) && (pb_result.stale())) ? true : false;
      pb_result_partial =
          ((pb_result.has_partial()) && (pb_result.partial())) ? true : false;

      for (int var = 0; var < pb_result.cell_size(); ++var) {
        const hbase::pb::Cell &cell = pb_result.cell(var);
        if (cell.has_cell_type()) {

          cells.push_back(
              Cell(
                  cell.row(),
                  cell.family(),
                  cell.qualifier(),
                  cell.timestamp(),
                  cell.value(),
                  cell.tags(),
                  cell.has_cell_type() ?
                      static_cast<CellType::cellType>(cell.cell_type()) :
                      CellType::UNKNOWN));
        }
      }
    }
  }

  DLOG(INFO) << cell_scanner->GetDataLength();
  this->GetCellsFromCellMetaBlock(cells, *cell_scanner);

  if (cells.size() > 0) {
    result = new Result(cells, pb_result_exists, pb_result_stale,
                        pb_result_partial);
  }

  return result;

}

void Table::GetMultiException(const std::string &exception,
                              const bool &stack_trace, const int &index,
                              const std::string &region, int &retry_count) {

  std::unique_ptr < HBaseException > exc(new HBaseException(exception, false));
  if (exc.get()) {

    if (this->CheckExceptionForRetries(*exc.get(), retry_count)) {
      LOG(INFO) << "Retry Exception [" << exception
          << "] received for multi request at index [ " << index
          << "for region " << region;
    } else {
      LOG(INFO) << "No retry for exception [" << exception
          << "]; occured @ index [" << index << "]; for region [" << region
          << "]";
    }
  }
}

/*
 Table::~Table(){
 DLOG(INFO) << "Calling Table Destructor" << std::endl;
 if(!this->isClosed)
 this->close();
 if(nullptr != this->tableName){
 delete this->tableName;
 this->tableName = nullptr;
 }
 }
 */

