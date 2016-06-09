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

#include <map>
#include <mutex>

#include <google/protobuf/message.h>

#include "server_node_connection.h"
#include "cell_scanner.h"
#include "configuration.h"
#include "sasl_global.h"

class ServerNodeConnection;

class RpcClient {
 public:
  RpcClient(Configuration *configuration, const std::string &user);
  ~RpcClient();
  void Init();
  std::unique_ptr<google::protobuf::Message> Call(
			const std::string &service_name,
			const std::string &method_name,
			const google::protobuf::Message &param,
			const std::string &host_name, const int &port,
			const std::string &user_name,
			google::protobuf::Message &default_response_type,
			CellScanner &cell_scanner);

  Configuration *GetConfiguration() const;

 private:
  std::map<std::size_t, ServerNodeConnection*> connections_;
  Configuration *configuration_;
  std::string user_;
  SaslGlobalData *sasl_global_data_;
  std::mutex connection_lookup_mutex_;
};
