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

#include "admin.h"

#include <iostream>
#include <vector>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <glog/logging.h>
#include "util.h"
#include "pb_request_builder.h"
#include "protobuf_util.h"
#include "../rpc/generated/Master.pb.h"
#include "../rpc/generated/RPC.pb.h"

Admin::~Admin() {

}

Admin::Admin(Connection *connection)
    : connection_(connection) {
  rpc_client_ = connection_->GetRpcClient();
}

Admin::Admin() {

}

void Admin::EnableTable(const std::string &table_name) {

  std::unique_ptr<google::protobuf::Message> req(
      ProtoBufRequestBuilder::CreateEnableTableRequest(table_name));

  std::string master_server_ip("");
  int master_server_port(0);
  this->connection_->FindMasterServer(master_server_ip, master_server_port);
  const std::string user_name(connection_->GetUser());
  const std::string service_name("MasterService");
  const std::string method_name("EnableTable");
  hbase::pb::EnableTableResponse response_type;
  google::protobuf::Message *def_resp_type = &response_type;
  CellScanner cell_scanner;

  std::unique_ptr<google::protobuf::Message> ptr_resp(
      rpc_client_->Call(service_name, method_name, *req, master_server_ip,
                        master_server_port, user_name, *def_resp_type,
                        cell_scanner));

  def_resp_type = ptr_resp.get();
  if (NULL != def_resp_type) {
    hbase::pb::EnableTableResponse *resp =
        dynamic_cast<hbase::pb::EnableTableResponse *>(def_resp_type);
    DLOG(INFO)<< "[" << __LINE__ << ":" << __func__ << "] DBG: Response:[" << resp->ByteSize() << "]: " << resp->DebugString();
    TableState::TableOperationStatus op_status = GetProcedureResponse(
        resp->proc_id());
    while (TableState::RUNNING == op_status) {
      usleep(sleep_betwee_statuschk);
      op_status = GetProcedureResponse(resp->proc_id());
    }

  } else {
    ;  //throw std::string("Exception Received");
  }
  return;
}

void Admin::DisableTable(const std::string &table_name) {

  std::unique_ptr<google::protobuf::Message> req(
      ProtoBufRequestBuilder::CreateDisableTableRequest(table_name));

  std::string master_server_ip("");
  int master_server_port(0);
  this->connection_->FindMasterServer(master_server_ip, master_server_port);
  const std::string user_name(connection_->GetUser());
  const std::string service_name("MasterService");
  const std::string method_name("DisableTable");
  hbase::pb::DisableTableResponse response_type;
  google::protobuf::Message *def_resp_type = &response_type;
  CellScanner cell_scanner;

  std::unique_ptr<google::protobuf::Message> ptr_resp = rpc_client_->Call(
      service_name, method_name, *req, master_server_ip, master_server_port,
      user_name, *def_resp_type, cell_scanner);

  def_resp_type = ptr_resp.get();
  if (NULL != def_resp_type) {
    hbase::pb::DisableTableResponse *resp =
        dynamic_cast<hbase::pb::DisableTableResponse *>(def_resp_type);
    DLOG(INFO)<< "[" << __LINE__ << ":" << __func__ << "] DBG: Response:[" << resp->ByteSize() << "]: " << resp->DebugString();
    TableState::TableOperationStatus op_status = GetProcedureResponse(
        resp->proc_id());
    while (TableState::RUNNING == op_status) {
      usleep(sleep_betwee_statuschk);
      op_status = GetProcedureResponse(resp->proc_id());
    }
  } else {
    ;  //throw std::string("Exception Received");
  }
  return;
}

void Admin::CheckTableState(const std::string &table_name) {

  hbase::pb::RequestHeader *rh = new hbase::pb::RequestHeader();
  SetRequestHeader(*rh, "GetTableState", true);
  int callId = Connection::GetRpcCallId();
  std::string namespace_value("");
  std::string tablename_value("");
  GetTableAndNamespace(table_name, namespace_value, tablename_value);

  hbase::pb::TableName table_obj;
  table_obj.set_namespace_(namespace_value);
  table_obj.set_qualifier(tablename_value);
  hbase::pb::GetTableStateRequest *reqObj =
      new hbase::pb::GetTableStateRequest();
  reqObj->set_allocated_table_name(&table_obj);

  int total_size = 0;
  total_size += rh->ByteSize();
  total_size += google::protobuf::io::CodedOutputStream::VarintSize32(
      rh->ByteSize());

  total_size += reqObj->ByteSize();
  total_size += google::protobuf::io::CodedOutputStream::VarintSize32(
      reqObj->ByteSize());

  int buffer_size = total_size + 4;
  char *packet = new char[buffer_size];
  ::memset(packet, '\0', buffer_size);

  google::protobuf::io::ArrayOutputStream aos(packet, buffer_size);
  google::protobuf::io::CodedOutputStream *coded_output =
      new google::protobuf::io::CodedOutputStream(&aos);

  unsigned int uiTotalSize = total_size;
  SwapByteOrder(uiTotalSize);
  coded_output->WriteRaw(&uiTotalSize, 4);
  coded_output->WriteVarint32(rh->ByteSize());
  bool success = rh->SerializeToCodedStream(coded_output);
  coded_output->WriteVarint32(reqObj->ByteSize());
  success = reqObj->SerializeToCodedStream(coded_output);
  DLOG(INFO)<< __LINE__ << ":" << __func__ << " [DBG] \n"<< reqObj->DebugString();

  int bytesSent = connection_->SendDataToServer(packet, aos.ByteCount());
  int recv_buff_size = 2048;
  char recv_buffer[recv_buff_size];

  int bytesRecvd = connection_->RecvDataFromServer(recv_buffer, recv_buff_size);

  char *poffset = recv_buffer;
  unsigned int *pbuffer = (unsigned int *) poffset;
  poffset += 4;
  SwapByteOrder(*pbuffer);
  uiTotalSize = *pbuffer;

  google::protobuf::io::ArrayInputStream arrInput(poffset, recv_buff_size - 4);
  google::protobuf::io::CodedInputStream input(&arrInput);

  hbase::pb::ResponseHeader resp_hdr;
  unsigned int resp_hdrMsgSize = 0;
  input.ReadVarint32(&resp_hdrMsgSize);
  google::protobuf::io::CodedInputStream::Limit limit = input.PushLimit(
      resp_hdrMsgSize);
  success = resp_hdr.ParseFromCodedStream(&input);
  input.PopLimit(limit);

  int callIdResp = Connection::GetRpcCallId();
  DLOG(INFO)<< __LINE__ << ":" << __func__ << " [DBG] "
  << " Has Exception: " << (resp_hdr.has_exception() ? "True" : "False")
  << " Has Meta Cell Block: " << (resp_hdr.has_cell_block_meta() ? "True" : "False")
  << " Response: " << resp_hdr.DebugString()
  << " Req:[" <<callId << "]: Resp:[" << callIdResp << "]"
  << " Req:[" << rh->call_id() << "]: Resp:[" << resp_hdr.call_id() << "]";
  if (!resp_hdr.has_exception()) {
    hbase::pb::GetTableStateResponse respObj;
    google::protobuf::uint32 respMsgSize = 0;
    input.ReadVarint32(&respMsgSize);
    limit = input.PushLimit(respMsgSize);
    success = respObj.ParseFromCodedStream(&input);
    input.PopLimit(limit);
    DLOG(INFO)<< __LINE__ << ":" << __func__ << " [DBG] \n"
    << respObj.DebugString();
    hbase::pb::TableState tableState = respObj.table_state();
    if (tableState.has_state()) {
      hbase::pb::TableState_State eState = tableState.state();
      switch (eState) {
        case hbase::pb::TableState::DISABLED: {
          DLOG(INFO)<< __LINE__ << ":" << __func__ << " [DBG] Table DISABLED";
          break;
        }
        case hbase::pb::TableState::ENABLED: {
          DLOG(INFO) << __LINE__ << ":" << __func__ << " [DBG] Table ENABLED";
          break;
        }
        case hbase::pb::TableState::DISABLING: {
          DLOG(INFO) << __LINE__ << ":" << __func__ << " [DBG] Table DISABLING";
          break;
        }
        case hbase::pb::TableState::ENABLING: {
          DLOG(INFO) << __LINE__ << ":" << __func__ << " [DBG] Table ENABLING";
          break;
        }
      }
    }
  } else {
    hbase::pb::ExceptionResponse excptResponse = resp_hdr.exception();
    LOG(WARNING) << __LINE__ << ":" << __func__ << " [DBG] "
    << " Exception from: " << excptResponse.exception_class_name();
  }
}

bool Admin::IsEnabledTable(const std::string &table_name) {

  CheckTableState(table_name);
}

bool Admin::IsDisabledTable(const std::string &table_name) {

  CheckTableState(table_name);
}

bool Admin::TableExists(const std::string &table_name) {

  CheckTableState(table_name);
}

void Admin::DeleteTable(const std::string &table_name) {

  std::unique_ptr<google::protobuf::Message> req(
      ProtoBufRequestBuilder::CreateDeleteTableRequest(table_name));

  std::string master_server_ip("");
  int master_server_port(0);
  this->connection_->FindMasterServer(master_server_ip, master_server_port);
  const std::string user_name(connection_->GetUser());
  const std::string service_name("MasterService");
  const std::string method_name("DeleteTable");
  hbase::pb::DeleteTableResponse response_type;
  google::protobuf::Message *def_resp_type = &response_type;
  CellScanner cell_scanner;

  std::unique_ptr<google::protobuf::Message> ptrResp = rpc_client_->Call(
      service_name, method_name, *req, master_server_ip, master_server_port,
      user_name, *def_resp_type, cell_scanner);

  def_resp_type = ptrResp.get();
  if (NULL != def_resp_type) {
    hbase::pb::DeleteTableResponse *resp =
        dynamic_cast<hbase::pb::DeleteTableResponse *>(def_resp_type);
    DLOG(INFO)<< "[" << __LINE__ << ":" << __func__ << "] DBG: Response:[" << resp->ByteSize() << "]: " << resp->DebugString();

    TableState::TableOperationStatus op_status = GetProcedureResponse(
        resp->proc_id());
    while (TableState::RUNNING == op_status) {
      usleep(sleep_betwee_statuschk);
      op_status = GetProcedureResponse(resp->proc_id());
    }

  } else {
    LOG(WARNING)<<"Exception Received";
  }
  return;
}

TableState::TableOperationStatus Admin::GetProcedureResponse(
    const int & procedureId) {

  std::unique_ptr<google::protobuf::Message> req(
      ProtoBufRequestBuilder::CreateProcedureRequest(procedureId));

  TableState::TableOperationStatus tableOpRet = TableState::NOT_FOUND;
  std::string master_server_ip("");
  int master_server_port(0);
  connection_->FindMasterServer(master_server_ip, master_server_port);
  const std::string user_name(connection_->GetUser());
  const std::string service_name("MasterService");
  const std::string method_name("getProcedureResult");
  hbase::pb::GetProcedureResultResponse response_type;
  google::protobuf::Message *def_resp_type = &response_type;
  CellScanner cell_scanner;

  std::unique_ptr<google::protobuf::Message> ptrResp(
      rpc_client_->Call(service_name, method_name, *req, master_server_ip,
                        master_server_port, user_name, *def_resp_type,
                        cell_scanner));

  def_resp_type = ptrResp.get();
  if (NULL != def_resp_type) {
    hbase::pb::GetProcedureResultResponse *resp =
        dynamic_cast<hbase::pb::GetProcedureResultResponse *>(def_resp_type);
    DLOG(INFO)<< "[" << __LINE__ << ":" << __func__ << "] DBG: Response:[" << resp->ByteSize() << "]: " << resp->DebugString();
    if (resp->has_state()) {
      hbase::pb::GetProcedureResultResponse_State respState = resp->state();
      switch (respState) {
        case hbase::pb::GetProcedureResultResponse_State_RUNNING: {
          tableOpRet = TableState::RUNNING;
          break;
        }
        case hbase::pb::GetProcedureResultResponse_State_FINISHED: {
          tableOpRet = TableState::FINISHED;
          break;
        }
        case hbase::pb::GetProcedureResultResponse_State_NOT_FOUND: {
          tableOpRet = TableState::NOT_FOUND;
          break;
        }
        default:
          tableOpRet = TableState::NOT_FOUND;
      }
    }
  } else {
    LOG(WARNING)<<"Exception Received";
  }
  return tableOpRet;
}

void Admin::ListTables(std::vector<TableSchema> &table_list,
                       const std::string &reg_ex,
                       const bool &include_sys_tables) {

  std::unique_ptr<google::protobuf::Message> req(
      ProtoBufRequestBuilder::CreateListTablesRequest(reg_ex,
                                                      include_sys_tables));

  std::string master_server_ip("");
  int master_server_port(0);
  this->connection_->FindMasterServer(master_server_ip, master_server_port);
  const std::string user_name(connection_->GetUser());
  const std::string service_name("MasterService");
  const std::string method_name("GetTableDescriptors");
  hbase::pb::GetTableDescriptorsResponse response_type;
  google::protobuf::Message *def_resp_type = &response_type;
  CellScanner cell_scanner;

  std::unique_ptr<google::protobuf::Message> ptrResp(
      rpc_client_->Call(service_name, method_name, *req, master_server_ip,
                        master_server_port, user_name, *def_resp_type,
                        cell_scanner));

  def_resp_type = ptrResp.get();
  if (NULL != def_resp_type) {
    hbase::pb::GetTableDescriptorsResponse *resp =
        dynamic_cast<hbase::pb::GetTableDescriptorsResponse *>(def_resp_type);
    DLOG(INFO)<< "[" << __LINE__ << ":" << __func__ << "] DBG: Response:[" << resp->ByteSize() << "]: " << resp->DebugString();
    int numOfTables = resp->table_schema_size();

    for (int i = 0; i < numOfTables; i++) {

      TableName *table_name = nullptr;
      if (resp->table_schema(i).has_table_name()) {
        hbase::pb::TableName tableObj = resp->table_schema(i).table_name();
        if (tableObj.has_namespace_() && tableObj.has_qualifier())
          table_name = TableName::CreateTableNameIfNecessary(
              tableObj.namespace_(), tableObj.qualifier());
      }
      TableSchema tbl_schema(table_name);
      for (int j = 0; j < resp->table_schema(i).attributes_size(); j++) {
        hbase::pb::BytesBytesPair bytesPair = resp->table_schema(i).attributes(
            j);
        if (bytesPair.has_first() && bytesPair.has_second()) {
          tbl_schema.SetValue(bytesPair.first(), bytesPair.second());
        }
      }
      for (int j = 0; j < resp->table_schema(i).column_families_size(); j++) {
        hbase::pb::ColumnFamilySchema pbColFamilySchema = resp->table_schema(i)
            .column_families(j);
        ColumnFamilySchema colFamSchema(
            pbColFamilySchema.has_name() ? pbColFamilySchema.name() : "");
        for (int k = 0; k < pbColFamilySchema.attributes_size(); k++) {
          hbase::pb::BytesBytesPair bytesPair = pbColFamilySchema.attributes(k);
          if (bytesPair.has_first() && bytesPair.has_second()) {
            colFamSchema.SetValue(bytesPair.first(), bytesPair.second());
          }
        }

        for (int k = 0; k < pbColFamilySchema.configuration_size(); k++) {
          hbase::pb::NameStringPair nameString =
              pbColFamilySchema.configuration(k);
          if (nameString.has_name() && nameString.has_value()) {
            colFamSchema.SetConfiguration(nameString.name(),
                                          nameString.value());
          }
        }

        tbl_schema.AddFamily(colFamSchema);
      }
      for (int j = 0; j < resp->table_schema(i).configuration_size(); j++) {
        hbase::pb::NameStringPair nameString = resp->table_schema(i)
            .configuration(j);
        if (nameString.has_name() && nameString.has_value()) {
          tbl_schema.SetConfiguration(nameString.name(), nameString.value());
        }
      }

      table_list.push_back(tbl_schema);
    }
  } else {
    LOG(WARNING)<<"Exception Received";
  }

  return;
}

void Admin::CreateTable(TableSchema &table_schema) {

  std::unique_ptr<google::protobuf::Message> req(
      ProtoBufRequestBuilder::CreateTableRequest(table_schema));

  std::string master_server_ip;
  int master_server_port;
  connection_->FindMasterServer(master_server_ip, master_server_port);

  const std::string user_name(connection_->GetUser());
  const std::string service_name("MasterService");
  const std::string method_name("CreateTable");
  hbase::pb::CreateTableResponse response_type;
  google::protobuf::Message *def_resp_type = &response_type;
  CellScanner cell_scanner;

  std::unique_ptr<google::protobuf::Message> ptrResp(
      rpc_client_->Call(service_name, method_name, *req, master_server_ip,
                        master_server_port, user_name, *def_resp_type,
                        cell_scanner));

  def_resp_type = ptrResp.get();
  if (NULL != def_resp_type) {
    hbase::pb::CreateTableResponse *resp =
        dynamic_cast<hbase::pb::CreateTableResponse *>(def_resp_type);
    DLOG(INFO)<< "[" << __LINE__ << ":" << __func__ << "] DBG: Response:[" << resp->ByteSize() << "]: " << resp->DebugString();
    TableState::TableOperationStatus op_status = GetProcedureResponse(
        resp->proc_id());
    while (TableState::RUNNING == op_status) {
      usleep(sleep_betwee_statuschk);
      op_status = GetProcedureResponse(resp->proc_id());
    }

  } else {
    LOG(WARNING)<<"Exception Received";
  }
  return;

}
