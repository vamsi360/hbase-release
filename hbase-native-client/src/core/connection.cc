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
 */

#include "connection.h"

#include <sstream>
#include <memory>
#include <stdio.h>
#include <Poco/ByteOrder.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <glog/logging.h>

#include "cell.h"
#include "util.h"
#include "utils.h"
#include "pb_request_builder.h"
#include "region_details.h"
#include "exception.h"
#include "../rpc/generated/RPC.pb.h"
#include "../rpc/generated/Master.pb.h"



using namespace Poco::Net;

int Connection::rpcCallId_ = 122;

int Connection::SetRpcCallId() {
  return Connection::rpcCallId_++;
}

int Connection::GetRpcCallId() {
  return Connection::rpcCallId_;
}

Connection::Connection(Configuration *configuration, std::string &user) :callId_(30), socket_(NULL),
    rpc_client_(std::make_shared<RpcClient>(configuration, user)) {
  configuration_ = configuration;
  meta_region_server_port_ = -1;
  master_server_port_ = -1;
  use_meta_cache_ = true;
  user_ = user;
}

Connection::~Connection() {
  if (NULL != socket_) {
    socket_->shutdownReceive();
    socket_->shutdownSend();
    socket_->close();
  }
}

void Connection::SetZkQuorum(const char *zk_q) { this->zk_quorum_ = zk_q; }

void Connection::EndianByteSwapper(char *dest, char const *src) {
  uint *p_dest = reinterpret_cast< uint* >(dest);
  uint const* const p_src = reinterpret_cast< uint const* >(src);
  *p_dest = (*p_src >> 24) | ((*p_src & 0x00ff0000) >> 8) | ((*p_src & 0x0000ff00) << 8) | (*p_src << 24);
}

void Connection::Init() {
  rpc_client_->Init();
  InitZK();
}

void Connection::InitZK() {
  std::string authType = configuration_->GetValue("hbase.security.authentication");
  bool isSecuredCluster = false;

  std::string zkZnodeParent = configuration_->GetValue("zookeeper.znode.parent");

  if (authType == "kerberos") {
    isSecuredCluster = true;
  }

  zk_ = new Zookeeper(zk_quorum_, isSecuredCluster, zkZnodeParent);
  zk_->Connect();
  while(!zk_->IsConnected()) {
    ::sleep(3);
  }
  closed_ = false;
}

std::shared_ptr<RpcClient> Connection::GetRpcClient() {
  return rpc_client_;
}

void Connection::Init(const std::string &hostName, unsigned long portNo,
    const std::string &serviceName) {
  SocketAddress sa(hostName, portNo);

  socket_ = new StreamSocket();
  socket_->connect(sa);

  char *preamble = new char[6];
  int i = 0;
  preamble[i++] = 'H';
  preamble[i++] = 'B';
  preamble[i++] = 'a';
  preamble[i++] = 's';
  preamble[i++] = 0;
  preamble[i++] = 80;

  int available = socket_->impl()->available();

  socket_->sendBytes((const void *)preamble, 6);

  hbase::pb::ConnectionHeader *conHeader = new hbase::pb::ConnectionHeader();
  std::string service_name(serviceName);
  conHeader->set_service_name(service_name);

  hbase::pb::UserInformation *userInfo = new hbase::pb::UserInformation();
  std::string *effective_user = new std::string(user_);

  userInfo->set_allocated_effective_user(effective_user);
  conHeader->set_allocated_user_info(userInfo);

  std::string codecClass("org.apache.hadoop.hbase.codec.KeyValueCodec");
  conHeader->set_allocated_cell_block_codec_class(&codecClass);

  hbase::pb::VersionInfo *versionInfo = new hbase::pb::VersionInfo();
  std::string version("2.0.0-SNAPSHOT");
  versionInfo->set_allocated_version(&version);
  versionInfo->set_version_major(2);
  versionInfo->set_version_minor(0);
  std::string versionUrl("git://localhost.localdomain/home/vmthattikota/source_control/hbase");
  versionInfo->set_allocated_url(&versionUrl);
  std::string revision("b5dcf5d412ad10b52601a671383d933129ee5c5e");
  versionInfo->set_allocated_revision(&revision);
  std::string user("vmthattikota");
  versionInfo->set_allocated_user(&user);
  std::string date("Tue Jan 26 12:46:34 PST 2016");
  versionInfo->set_allocated_date(&date);
  std::string srcChecksum("e0e165015da2a07fb5aa2ac3dfb61de6");
  versionInfo->set_allocated_src_checksum(&srcChecksum);

  conHeader->set_allocated_version_info(versionInfo);

  int totalSize = conHeader->ByteSize();

  int bufferSize = totalSize + 4;
  google::protobuf::uint8 *packet = new google::protobuf::uint8[bufferSize];
  ::memset(packet, '\0', bufferSize);

  google::protobuf::io::ArrayOutputStream aos(packet, bufferSize);
  google::protobuf::io::CodedOutputStream *coded_output = new google::protobuf::io::CodedOutputStream(&aos);

  unsigned int uiTotalSize = totalSize;
  SwapByteOrder(uiTotalSize);
  coded_output->WriteRaw(&uiTotalSize, 4);
  bool success = conHeader->SerializeToCodedStream(coded_output);

  int bytesSent = socket_->sendBytes(packet, aos.ByteCount());
}

bool Connection::IsMasterRunning() {
  int available = socket_->impl()->available();

  hbase::pb::RequestHeader *rh = new hbase::pb::RequestHeader();
  rh->set_call_id(Connection::rpcCallId_++);
  std::string *methodName = new std::string("IsMasterRunning");
  rh->set_allocated_method_name(methodName);
  rh->set_request_param(true);

  hbase::pb::IsMasterRunningRequest *imrr = new hbase::pb::IsMasterRunningRequest();
  int totalSize = 0;
  totalSize += rh->ByteSize();
  totalSize += google::protobuf::io::CodedOutputStream::VarintSize32(rh->ByteSize());

  totalSize += imrr->ByteSize();
  totalSize += google::protobuf::io::CodedOutputStream::VarintSize32(imrr->ByteSize());

  int bufferSize = totalSize + 4;
  char *packet = new char[bufferSize];
  ::memset(packet, '\0', bufferSize);

  google::protobuf::io::ArrayOutputStream aos(packet, bufferSize);
  google::protobuf::io::CodedOutputStream *coded_output = new google::protobuf::io::CodedOutputStream(&aos);

  unsigned int uiTotalSize = totalSize;
  SwapByteOrder(uiTotalSize);
  coded_output->WriteRaw(&uiTotalSize, 4);

  coded_output->WriteVarint32(rh->ByteSize());
  bool success = rh->SerializeToCodedStream(coded_output);
  coded_output->WriteVarint32(imrr->ByteSize());
  success = imrr->SerializeToCodedStream(coded_output);

  int bytesSent = socket_->sendBytes(packet, aos.ByteCount());

  char *buffer = new char[2048];
  char *pOffset = buffer;
  int bytesReceived;

  int remBufferSize = 2048;
  bytesReceived = socket_->receiveBytes(pOffset, remBufferSize);
  unsigned int *pBuffer = (unsigned int *)pOffset;
  pOffset += 4;
  SwapByteOrder(*pBuffer);
  uiTotalSize = *pBuffer;

  google::protobuf::io::ArrayInputStream arr(pOffset, remBufferSize - 4);
  google::protobuf::io::CodedInputStream input(&arr);

  hbase::pb::ResponseHeader msg1;
  unsigned int message1_size = 0;
  input.ReadVarint32(&message1_size);
  google::protobuf::io::CodedInputStream::Limit limit = input.PushLimit(message1_size);
  success = msg1.ParseFromCodedStream(&input);
  input.PopLimit(limit);

  hbase::pb::IsMasterRunningResponse msg2;
  unsigned int message2_size = 0;
  input.ReadVarint32(&message2_size);
  limit = input.PushLimit(message2_size);
  success = msg2.ParseFromCodedStream(&input);
  input.PopLimit(limit);

  return true;
}

int Connection::SendDataToServer(char *sendBuffer, int sendByteSize) {
  int bytesSent = socket_->sendBytes(sendBuffer, sendByteSize);
  DLOG(INFO)  << "Bytes Sent: " << bytesSent ;
  return bytesSent;
}

int Connection::RecvDataFromServer(char *recvBuffer, int recvByteSize) {
  int bytesReceived = socket_->receiveBytes(recvBuffer, recvByteSize);
  DLOG(INFO)  << "Bytes Rcvd: " << recvByteSize ;
  return bytesReceived;
}

//TODO Error habdling reqd
int Connection::RecvDataFromServer(std::vector<char> &vectNwPkt, int recvByteSize) {

  try {
    while (true) {
      char sockBuffer[recvByteSize];
      int bytesReceived = socket_->receiveBytes(&sockBuffer, recvByteSize);
      DLOG(INFO)  	<< "[DBG] " << bytesReceived << "/"
          << recvByteSize << " Bytes Recvd" ;
      vectNwPkt.insert (vectNwPkt.end(), sockBuffer, sockBuffer+bytesReceived);
      if ((bytesReceived < recvByteSize) || (0 == bytesReceived) ) {
        break;
      }
    }
  } catch(...) {
    DLOG(INFO) << "Exception on recv";
  }
  return vectNwPkt.size();
}

void Connection::FindMetaRegionServer(std::string &metaRegionServerName, int &port) {
  if (-1 == meta_region_server_port_) {
    zk_->LocateHBaseMeta(meta_region_server_ip_, meta_region_server_port_);
  }

  metaRegionServerName = meta_region_server_ip_;
  port = meta_region_server_port_;

  DLOG(INFO)  << "MetaRegionServer Location: "
      << metaRegionServerName;
  DLOG(INFO)  << "MetaRegionServer IP: " << port ;
}

RegionDetails* Connection::FindTheRegionServerForTheTable (
    const std::string &tableName, std::string &rowKey) {
  std::shared_ptr<TableName> pTN(TableName::ValueOf(tableName));
  RegionDetails *pRegionDetails = nullptr;

  if (use_meta_cache_) {
    pRegionDetails = meta_cache_.LookupRegion(*pTN.get(), rowKey);
  }

  if (nullptr == pRegionDetails) {
    DLOG(INFO) << "Region Details not found in cache. Will locate region from meta server." << std::endl;
    pRegionDetails = LocateTableRegionInMeta(tableName, rowKey);

    if (use_meta_cache_) {
      meta_cache_.CacheRegion(*pTN, pRegionDetails);
    }
  } else {
    DLOG(INFO) << "Region Details found in cache." << std::endl;
  }

  return pRegionDetails;
}

void Connection::FindTheRegionServerForTheTable(const std::string &tableName,
    std::string &rowKey, std::string &tableRegionInfo,
    std::string &tableRegionIP, int &tableRsPort) {

  RegionDetails *pRegionDetails = FindTheRegionServerForTheTable(tableName, rowKey);

  if (pRegionDetails) {
    tableRsPort = pRegionDetails->GetRegionServerPort();
    tableRegionIP = pRegionDetails->GetRegionServerIpAddr();
    tableRegionInfo = const_cast<std::string&>(pRegionDetails->GetRegionName());
  }
  DLOG(INFO)  << "RegionServer Info: " << tableRegionInfo;
  DLOG(INFO)  << "Table RegionServer Location: " << tableRegionIP;
  DLOG(INFO)  << "Table RegionServer IP: " << tableRsPort;
}

RegionDetails* Connection::FindTheRegionServerForTheTable (const TableName *tableName, const std::string &rowKey) {

  RegionDetails *pRegionDetails = nullptr;

  if (use_meta_cache_) {
    pRegionDetails =  meta_cache_.LookupRegion(*tableName, rowKey);
  }

  if (nullptr == pRegionDetails) {
    DLOG(INFO) << "Region Details not found in cache. Will locate region from meta server." << std::endl;
    pRegionDetails = LocateTableRegionInMeta(tableName->GetName(), rowKey);

    if (use_meta_cache_) {
      meta_cache_.CacheRegion(*tableName, pRegionDetails);
    }
  } else {
    DLOG(INFO) << "Region Details found in cache." << std::endl;
  }

  return pRegionDetails;
}

void Connection::FindTheRegionServerForTheTable(const TableName *tableName,
    std::string &rowKey, std::string &tableRegionInfo,
    std::string &tableRegionIP, int &tableRsPort) {

  RegionDetails *pRegionDetails = FindTheRegionServerForTheTable(tableName, rowKey);

  if (pRegionDetails) {
    tableRsPort = pRegionDetails->GetRegionServerPort();
    tableRegionIP = pRegionDetails->GetRegionServerIpAddr();
    tableRegionInfo = const_cast<std::string&>(pRegionDetails->GetRegionName());
  }

  DLOG(INFO)  << "RegionServer Details: Info:" << tableRegionInfo << std::endl;
  DLOG(INFO)  << "[" << tableRegionIP << ":" <<  tableRsPort << "];" << std::endl;
}

void Connection::FindMasterServer(std::string &master_server_name, int &master_server_port) {
  master_server_name = "";
  master_server_port = 0;

  if (-1 == master_server_port_) {
    this->zk_->LocateHBaseMaster(master_server_ip_, master_server_port_);
  }

  master_server_port = master_server_port_;
  master_server_name = master_server_ip_;
}

RegionDetails* Connection::LocateRegionInMeta(std::string &meta_region_ip, int &meta_region_port,
    std::string &regionName, Scan &scan, int &numberOfRows, bool &closeScanner) {

  std::unique_ptr<google::protobuf::Message> scanRequest(ProtoBufRequestBuilder::CreateScanRequest(regionName,
      scan, numberOfRows, closeScanner));
  DLOG(INFO)  << "Byte Size::-> " << scanRequest->ByteSize();
  DLOG(INFO)  << "Request ::-> " << scanRequest->DebugString();

  hbase::pb::ScanResponse response_type;
  google::protobuf::Message *def_resp_type = &response_type;

  CellScanner cell_scanner;

  const std::string user_name(user_);
  const std::string service_name("ClientService");
  const std::string method_name("Scan");

  rpc_client_->Call(service_name, method_name, *scanRequest,
      meta_region_ip, meta_region_port,
      user_name, *def_resp_type, cell_scanner);

  char *cellBlock = cell_scanner.GetData();
  int size = cell_scanner.GetDataLength();

  ByteBuffer region_data(cellBlock, cellBlock + size);
  RegionDetails *pRegionDetails = RegionDetails::Parse(region_data);
  return pRegionDetails;
}

RegionDetails* Connection::LocateTableRegionInMeta(const std::string &tableName,
    const std::string &rowKey, const bool get_replicas) {

  RegionDetails *pRegionDetails = nullptr;
  std::string metaRegionServerName("");
  int mrsPort = 0;

  FindMetaRegionServer(metaRegionServerName, mrsPort);

  std::string regionId("99999999999999");
  std::string startKey = rowKey;
  std::string metaKey = tableName + "," + startKey + "," + regionId;

  Scan scan;
  bool reversed = true;
  scan.SetReversed(reversed);
  scan.SetStartRowVal(metaKey);
  bool small = true;
  scan.SetSmall(small);
  int caching = 1;
  scan.SetCaching(caching);
  int maxVersions = 1;
  scan.SetMaxVersions(maxVersions);

  int numberOfRows = 1;
  bool closeScanner = true;

  std::string regionName("hbase:meta,,1");
  std::string serviceName("ClientService");

  std::unique_ptr<google::protobuf::Message> scanRequest(
      ProtoBufRequestBuilder::CreateScanRequest(regionName,
          scan, numberOfRows, closeScanner));
  DLOG(INFO)  << "Byte Size::-> " << scanRequest->ByteSize();
  DLOG(INFO)  << "Request ::-> "  << scanRequest->DebugString();

  hbase::pb::ScanResponse response_type;
  google::protobuf::Message *def_resp_type = &response_type;

  CellScanner cell_scanner;

  const std::string user_name(user_);
  const std::string service_name("ClientService");
  const std::string method_name("Scan");

  std::unique_ptr<google::protobuf::Message>ptrResp(rpc_client_->Call(service_name, method_name, *scanRequest,
      metaRegionServerName, mrsPort, user_name, *def_resp_type,
      cell_scanner));

  def_resp_type = ptrResp.get();
  if (nullptr != def_resp_type) {
    DLOG(INFO) << def_resp_type->DebugString();
    char *cellBlock = cell_scanner.GetData();
    int size = cell_scanner.GetDataLength();


    ByteBuffer region_data(cellBlock, cellBlock + size);
    pRegionDetails = RegionDetails::Parse(region_data, get_replicas);

    if(pRegionDetails->GetTableName().GetName() != tableName){
      delete pRegionDetails;
      std::stringstream error_str;
      error_str <<"Table not found exception: Table '" << tableName << "' was not found, got: "
          << pRegionDetails->GetTableName().GetName() << std::endl;
      LOG(ERROR) << error_str.str();

      throw HBaseException(error_str.str());
    }
  }

  return pRegionDetails;
}

Admin* Connection::GetAdmin() {
  Admin *admin = new Admin(this);
  return admin;
}

void Connection::Close() {

  delete zk_;
  closed_ = true;

}

bool Connection::IsClosed() {

  return this->closed_;
}

Table* Connection::GetTable(TableName *tableName) {

  Table *table = new Table(tableName, this);
  return table;

}

bool Connection::InvalidateRegionServerCache(const TableName &table_name) {

  return meta_cache_.InvalidateCache(table_name);
}

const int &Connection::GetClientRetries() {

  return this->client_retries_;
}

void Connection::SetClientRetries(const int &client_retries) {

  this->client_retries_ = client_retries;
}


RegionDetails* Connection::GetReplicaRegions (const TableName *tableName, const std::string &rowKey) {

  RegionDetails *pRegionDetails = nullptr;

  if (use_meta_cache_) {
    ;
  }

  if (nullptr == pRegionDetails) {
    DLOG(INFO) << "Region Details not found in cache. Will locate region from meta server." << std::endl;
    pRegionDetails = LocateTableRegionInMeta(tableName->GetName(), rowKey, true);

    if (use_meta_cache_) {
      ;
    }
  } else {
    DLOG(INFO) << "Region Details found in cache." << std::endl;
  }

  return pRegionDetails;
}

void Connection::SetUser(const std::string &user) {
  user_ = user;
}

const std::string& Connection::GetUser() {
  return user_;
}
