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

#include "table_name.h"
#include "bytes.h"
#include "cell_scanner.h"

using REGION_SERVER_DETAILS = std::pair<std::string, int>;

class RegionDetails {
 public:
  RegionDetails();
  ~RegionDetails();
  static RegionDetails* Parse(const ByteBuffer &byte_buffer, const bool get_replicas = false);
  const std::string& GetEndKey() const;
  void SetEndKey(const std::string &endKey);
  bool IsOffline() const;
  void SetOffline(bool offline);
  unsigned long GetRegionId() const;
  void SetRegionId(unsigned long regionId);
  const std::string& GetRegionName() const;
  void SetRegionName(const std::string &regionName);
  void SetRegionName(const char *regionName);
  const std::string& GetRegionServerIpAddr() const;
  void SetRegionServerIpAddr(const std::string &regionServerIpAddr);
  void SetRegionServerIpAddr(const char *regionServerIpAddr);
  int GetRegionServerPort() const;
  void SetRegionServerPort(int regionServerPort);
  unsigned int GetReplicaId() const;
  void SetReplicaId(unsigned int replicaId);
  bool IsSplit() const;
  void SetSplit(bool split);
  const std::string& GetStartKey() const;
  void SetStartKey(const std::string &startKey);
  const TableName& GetTableName() const;
  void SetTableName(const TableName*tableName);
  unsigned long GetServerStartCode() const;
  void SetServerStartCode(unsigned long serverStartCode);
  void AddReplicaRegionServers(const std::string &ip_address, const int &port);
  const std::vector<REGION_SERVER_DETAILS> &GetReplicaRegionServers();
  bool operator < (const RegionDetails &rhs) const;
  static void GetCellsFromCellMetaBlock( const CellScanner &cell_scanner);

 private:
  std::string region_server_ip_addr_;
  int region_server_port_;
  std::vector<REGION_SERVER_DETAILS> region_server_details_;
  std::string region_name_;
  unsigned long region_id_;
  TableName* table_name_;
  std::string start_key_;
  std::string end_key_;
  bool offline_;
  bool split_;
  unsigned int replica_id_;
  unsigned long server_start_code_;
};
