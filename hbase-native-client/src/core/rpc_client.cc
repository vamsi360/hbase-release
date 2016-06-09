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

#include <memory>
#include <utility>

#include <Poco/Hash.h>
#include <google/protobuf/message.h>

#include "rpc_client.h"

RpcClient::RpcClient(Configuration *configuration, const std::string &user) {
  configuration_ = configuration;
  user_ = user;
  sasl_global_data_ = new SaslGlobalData(user_);
}

RpcClient::~RpcClient() {
  for (std::map<std::size_t,ServerNodeConnection*>::iterator it=connections_.begin();
      it!=connections_.end(); ++it) {
    delete it->second;
    connections_.erase(it);
  }

  delete sasl_global_data_;
}

void RpcClient::Init() {
  std::string authType = configuration_->GetValue(
      "hbase.security.authentication");
  bool isSecuredCluster = false;

  if (authType == "kerberos") {
    isSecuredCluster = true;
  }

  if (isSecuredCluster) {
    sasl_global_data_->Init();
  }
}

std::unique_ptr<google::protobuf::Message> RpcClient::Call(
    const std::string &service_name,
    const std::string &method_name,
    const google::protobuf::Message &param,
    const std::string &host_name, const int &port,
    const std::string &user_name,
    google::protobuf::Message &default_response_type,
    CellScanner &cell_scanner) {

  std::size_t val1 = Poco::hash(host_name);
  std::size_t val2 = Poco::hash(port);
  std::size_t val3 = Poco::hash(service_name);
  std::size_t val4 = Poco::hash(user_name);

  std::size_t val = val1 ^ val2 ^ val3 ^ val4;

  ServerNodeConnection *ptrsnc = nullptr;

  {
    //Lock it with mutex so that two threads try to lookup
    //the same region server and possibly create two
    //connection instances to the same region server
    std::lock_guard < std::mutex > lock(connection_lookup_mutex_);
    std::map<std::size_t, ServerNodeConnection*>::iterator it =
        connections_.find(val);

    if (it != connections_.end()) {
      ptrsnc = it->second;
    } else {
      ptrsnc = new ServerNodeConnection(this, host_name, port,
          service_name, user_name);
      std::pair<std::size_t, ServerNodeConnection*> kvPair(val, ptrsnc);
      connections_.insert(kvPair);
    }
  }
  return ptrsnc->CallMethod(method_name, param, default_response_type, cell_scanner);
}

Configuration* RpcClient::GetConfiguration() const {
  return configuration_;
}


