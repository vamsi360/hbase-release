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


#include "server_node_connection.h"

#include <memory>
#include <google/protobuf/message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <glog/logging.h>


#include "../rpc/generated/RPC.pb.h"
#include "../rpc/generated/Master.pb.h"
#include "return_codes.h"
#include "exception.h"
#include "sasl_client_handler.h"
#include "utils.h"

using namespace Poco::Net;

::google::protobuf::uint32 ServerNodeConnection::rpc_call_id_ = 0;

ServerNodeConnection::ServerNodeConnection(RpcClient *rpc_client, const std::string &host_name,
    const int &port, const std::string &service_name,
    const std::string &user_name):socket_(nullptr) {
  rpc_client_ = rpc_client;
  connected_ = false;
  host_name_ = host_name;
  user_name_ = user_name;
  port_ = port;
  service_name_ = service_name;
  use_sasl_ = false;
  MAX_RETRIES_ = 35;
  sasl_client_handler_ = nullptr;
}

ServerNodeConnection::~ServerNodeConnection() {
  if (connected_) {
    delete socket_;
    socket_ = NULL;
  }

  if (nullptr != sasl_client_handler_) {
    delete sasl_client_handler_;
  }
}

void ServerNodeConnection::EnsureConnected() {
  if (connected_) {
    return;
  }

  std::string authType = rpc_client_->GetConfiguration()->GetValue(
      "hbase.security.authentication");

  if (authType == "kerberos") {
    use_sasl_ = true;
  }

  SocketAddress sa(host_name_, port_);

  if(nullptr == socket_)
    socket_ = new StreamSocket();

  int retries = 0;
  bool connect_after_retries = false;
  do {
    try {
      Poco::Timespan ts(5, 0);
      socket_->connect(sa, ts);
      connect_after_retries = true;
    } catch (Poco::TimeoutException &toe) {
      retries += 1;
    }
  } while ((retries <= MAX_RETRIES_) && (connect_after_retries == false));

  if (!connect_after_retries) {
    std::ostringstream oss;
    oss << "Failure to connect to " << host_name_ << ":" << port_;
    throw oss.str();
  }

  socket_->setKeepAlive(true);

  char SIMPLE = 80;
  char KERBEROS = 81;
  char TOKEN = 82;
  char code = SIMPLE;

  if (use_sasl_) {
    code = KERBEROS;
  }

  char *preamble = new char[6];
  int i = 0;
  preamble[i++] = 'H';
  preamble[i++] = 'B';
  preamble[i++] = 'a';
  preamble[i++] = 's';
  preamble[i++] = 0;
  preamble[i++] = code;

  socket_->sendBytes((const void *)preamble, 6);
  delete []preamble;

  if (use_sasl_) {
    sasl_client_handler_ = new SaslClientHandler (user_name_, socket_, host_name_, port_);
    bool authenticated = sasl_client_handler_->Authenticate();
    if (!authenticated) {
      char *message = "Kerberos authentication failed";
      LOG(FATAL) << message;
      throw message;
    }
  }

  std::unique_ptr<hbase::pb::ConnectionHeader> conHeader(new hbase::pb::ConnectionHeader());
  std::string service_name(service_name_);
  conHeader->set_service_name(service_name);

  hbase::pb::UserInformation *userInfo = new hbase::pb::UserInformation();
  std::string *effective_user = new std::string(user_name_);
  //	userInfo->set_effective_user(effective_user);

  userInfo->set_allocated_effective_user(effective_user);
  conHeader->set_allocated_user_info(userInfo);

  std::string *codecClass = new std::string("org.apache.hadoop.hbase.codec.KeyValueCodec");
  conHeader->set_allocated_cell_block_codec_class(codecClass);

  hbase::pb::VersionInfo *versionInfo = new hbase::pb::VersionInfo();
  std::string version("2.0.0-SNAPSHOT");
  versionInfo->set_version(version);
  versionInfo->set_version_major(2);
  versionInfo->set_version_minor(0);
  std::string versionUrl("git://localhost.localdomain/home/vmthattikota/source_control/hbase");
  versionInfo->set_url(versionUrl);
  std::string revision("b5dcf5d412ad10b52601a671383d933129ee5c5e");
  versionInfo->set_revision(revision);
  std::string user(user_name_);
  versionInfo->set_user(user);
  std::string date("Tue Jan 26 12:46:34 PST 2016");
  versionInfo->set_date(date);
  std::string srcChecksum("e0e165015da2a07fb5aa2ac3dfb61de6");
  versionInfo->set_src_checksum(srcChecksum);

  conHeader->set_allocated_version_info(versionInfo);

  int totalSize = conHeader->ByteSize();

  int bufferSize = totalSize + 4;
  google::protobuf::uint8 *packet = new google::protobuf::uint8[bufferSize];
  ::memset(packet, '\0', bufferSize);

  google::protobuf::io::ArrayOutputStream aos(packet, bufferSize);
  google::protobuf::io::CodedOutputStream *coded_output = new google::protobuf::io::CodedOutputStream(&aos);

  unsigned int uiTotalSize = totalSize;
  CommonUtils::SwapByteOrder(uiTotalSize);
  coded_output->WriteRaw(&uiTotalSize, 4);
  bool success = conHeader->SerializeToCodedStream(coded_output);

  int bytesSent = socket_->sendBytes(packet, aos.ByteCount());
  delete []packet;
  delete coded_output;
  if (bytesSent != bufferSize) {
    throw "Incorrect bytes sent";
  }

  connected_ = true;
}

std::unique_ptr<google::protobuf::Message> ServerNodeConnection::
CallMethod(const std::string &method_name,
    const google::protobuf::Message &param,
    google::protobuf::Message &default_response_type,
    CellScanner &cell_scanner) {

  EnsureConnected();

  std::unique_ptr<hbase::pb::RequestHeader>rh(new hbase::pb::RequestHeader());
  {
    std::lock_guard < std::mutex > lock(serialize_rpc_call_mutex_);
    rh->set_call_id(ServerNodeConnection::rpc_call_id_++);
  }
  std::string *methodName = new std::string(method_name);
  rh->set_allocated_method_name(methodName);
  rh->set_request_param(true);

  int totalSize = 0;
  totalSize += rh->ByteSize();
  totalSize += google::protobuf::io::CodedOutputStream::VarintSize32(rh->ByteSize());

  totalSize += param.ByteSize();
  totalSize += google::protobuf::io::CodedOutputStream::VarintSize32(param.ByteSize());

  int bufferSize = totalSize + 4;
  char *packet = new char[bufferSize];
  ::memset(packet, '\0', bufferSize);

  google::protobuf::io::ArrayOutputStream aos(packet, bufferSize);
  google::protobuf::io::CodedOutputStream *coded_output = new google::protobuf::io::CodedOutputStream(&aos);

  unsigned int uiTotalSize = totalSize;
  CommonUtils::SwapByteOrder(uiTotalSize);
  coded_output->WriteRaw(&uiTotalSize, 4);
  coded_output->WriteVarint32(rh->ByteSize());
  bool success = rh->SerializeToCodedStream(coded_output);
  coded_output->WriteVarint32(param.ByteSize());
  success = param.SerializeToCodedStream(coded_output);

  unsigned int totalBytesReceived = 0;
  std::vector<char> vectNwPkt;
  int bytesReceived = 0;

  {
    std::lock_guard < std::mutex > lock(serialize_rpc_call_mutex_);

    int bytesSent = socket_->sendBytes(packet, aos.ByteCount());
    DLOG(INFO)<< "[" << __LINE__ << ":" << __func__ << "] DBG: Request:[" << bytesSent << "]: ";
    DLOG(INFO)<< rh->DebugString() << param.DebugString();

    int recvBuffSize = 1360;
    char recvBuffer[recvBuffSize];

    char recvBufferLength[4];

    bytesReceived = socket_->receiveBytes(recvBufferLength, 4);
    unsigned int *pBuffer = (unsigned int*) recvBufferLength;

    CommonUtils::SwapByteOrder(*pBuffer);
    uiTotalSize = *pBuffer;
    totalBytesReceived = uiTotalSize;

    while (uiTotalSize != 0) {
      bytesReceived = socket_->receiveBytes(recvBuffer, recvBuffSize);
      vectNwPkt.insert(vectNwPkt.end(), recvBuffer,
          recvBuffer + bytesReceived);
       uiTotalSize -= bytesReceived;
    }
  }

  char *data = &vectNwPkt[0];
  google::protobuf::io::ArrayInputStream arrInput(data, totalBytesReceived);

  google::protobuf::io::CodedInputStream input(&arrInput);

  hbase::pb::ResponseHeader resp_header;

  unsigned int message1_size = 0;
  input.ReadVarint32(&message1_size);
  google::protobuf::io::CodedInputStream::Limit limit = input.PushLimit(message1_size);
  success = resp_header.ParseFromCodedStream(&input);
  input.PopLimit(limit);

  delete []packet;
  delete coded_output;
  if(resp_header.has_exception()){
    throw HBaseException(resp_header.exception().stack_trace(), true);
  }

  google::protobuf::Message *pmsg = (google::protobuf::Message *)0;

  if (!resp_header.has_exception()) {
    pmsg = default_response_type.New();
    google::protobuf::uint32  message_size = 0;
    input.ReadVarint32(&message_size);
    limit = input.PushLimit(message_size);
    success = pmsg->ParseFromCodedStream(&input);
    input.PopLimit(limit);
    DLOG(INFO)  << "[" << __LINE__ << ":" << __func__ << "] DBG: Response:[" << bytesReceived << "]: ";
    DLOG(INFO)  << resp_header.DebugString() << pmsg->DebugString() ;

    char *cellBlock = nullptr;

    if (resp_header.has_cell_block_meta()) {
      hbase::pb::CellBlockMeta cbMeta = resp_header.cell_block_meta();
      int size = cbMeta.length();

      cellBlock = new char[size]; // This cell block will be delete once the cell_scanner goes out of scope
      std::memset(cellBlock, '\0', size);
      bool success = input.ReadRaw(cellBlock, size);
      cell_scanner.SetData(cellBlock, size);
    }

    std::unique_ptr<google::protobuf::Message> msg (pmsg);
    return msg;
  }
  std::unique_ptr<google::protobuf::Message> msg;
  return msg;
}
