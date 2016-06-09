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

#include <string>
#include <memory>
#include <mutex>
#include <google/protobuf/message.h>
#include <Poco/Net/StreamSocket.h>

#include "cell_scanner.h"
#include "rpc_client.h"
#include "sasl_client_handler.h"

class RpcClient;

class ServerNodeConnection {
 public:
  ServerNodeConnection(RpcClient *rpc_client, const std::string &host_name,
			const int &port, const std::string &service_name,
			const std::string &user_name);
  ~ServerNodeConnection();
  std::unique_ptr<google::protobuf::Message>
		CallMethod(
				const std::string &method_name,
				const google::protobuf::Message &param,
				google::protobuf::Message &default_response_type,
				CellScanner &cell_scanner);

 private:
  void EnsureConnected();
  bool connected_;
  std::string host_name_;
  int port_;
  std::string service_name_;
  std::string user_name_;
  Poco::Net::StreamSocket *socket_;
  RpcClient *rpc_client_;
  bool use_sasl_;
  int MAX_RETRIES_;
  SaslClientHandler *sasl_client_handler_;
  std::mutex serialize_rpc_call_mutex_;
  static ::google::protobuf::uint32 rpc_call_id_;
};
