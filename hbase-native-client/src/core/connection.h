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

#pragma once

#include <Poco/Net/StreamSocket.h>
#include "scan.h"
#include "rpc_client.h"
#include "../zk/zookeeper.h"

#include "table.h"
#include "admin.h"
#include "configuration.h"
#include "table_name.h"
#include "region_details.h"
#include "meta_cache.h"

#include <stdint.h>
#include <memory>

const int SOCKET_RECV_BYTESIZE = 1360;

class Table;
class Admin;

class Connection {

 public:
  Connection(Configuration *configuration, std::string &user);
  std::shared_ptr<RpcClient> GetRpcClient();
  virtual ~Connection();
  void Init();
  void SetZkQuorum(const char *zk_q);
  void Init(const std::string &hostName, unsigned long portNo,
      const std::string &serviceName);

  void FindTheRegionServerForTheTable(const std::string &tableName, std::string &rowKey,
  		std::string &tableRegionInfo, std::string &tableRegionIP, int &tableRsPort);
  RegionDetails* FindTheRegionServerForTheTable(const std::string &tableName,
      std::string &rowKey);
  void FindMasterServer(std::string &masterServerName, int &port);
  bool InvalidateRegionServerCache(const TableName &table_name);

  void SetUser(const std::string & user);
  const std::string &GetUser();

  static int SetRpcCallId();
  static int GetRpcCallId();
  bool IsMasterRunning();

  Table* GetTable(const std::string &tableName);
  Table* GetTable(TableName *);
  Admin* GetAdmin();
  int SendDataToServer(char *packet, int packetByteSize);
  int RecvDataFromServer(char *recvBuffer, int recvByteSize);
  int RecvDataFromServer(std::vector<char> &vectNwPkt, int recvByteSize = SOCKET_RECV_BYTESIZE);
  void Close();
  bool IsClosed();

  void FindTheRegionServerForTheTable(const TableName *tableName,
                                                    std::string &rowKey, std::string &tableRegionInfo,
                                                    std::string &tableRegionIP, int & tableRsPort);
  RegionDetails* FindTheRegionServerForTheTable (
      const TableName *tableName, const std::string &rowKey) ;
  const int & GetClientRetries();
  void SetClientRetries(const int &client_retries);
  RegionDetails *GetReplicaRegions (const TableName *tableName,
                                  const std::string &rowKey);

private:
  void InitZK();
  void EndianByteSwapper(char *dest, char const *src);
  char *GetQualifier(char *pSrc);

  void FindMetaRegionServer(std::string &metaRegionServerName, int &port);
  RegionDetails* LocateRegionInMeta(std::string &meta_region_ip, int & meta_region_port,
		  std::string &regionName, Scan & scan, int &numberOfRows, bool &closeScanner);

  RegionDetails* LocateTableRegionInMeta(const std::string &tableName, const std::string &rowKey, const bool get_replicas = false);
  int LocateReplicaRegionsInMeta(const std::string &tableName,
                              const std::string &rowKey, std::vector<RegionDetails*> &replica_regions);

  std::string zk_quorum_;
  Poco::Net::StreamSocket *socket_;
  unsigned long callId_;
  static int rpcCallId_;
  std::shared_ptr<RpcClient> rpc_client_;
  Zookeeper *zk_;
  bool closed_;
  Configuration *configuration_;
  std::string user_;
  std::string meta_region_server_ip_;
  int meta_region_server_port_;
  std::string master_server_ip_;
  int master_server_port_;
  bool use_meta_cache_;
  MetaCache meta_cache_;
  int client_retries_;

};
