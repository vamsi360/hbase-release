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


#include "region_details.h"

#include <glog/logging.h>
#include <stdio.h>
#include <memory>
#include <Poco/ByteOrder.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include "cell.h"
#include "../rpc/generated/RPC.pb.h"
#include "utils.h"

RegionDetails::RegionDetails():region_server_ip_addr_(""), region_server_port_(0) {

}

RegionDetails::~RegionDetails() {

}

RegionDetails* RegionDetails::Parse(const ByteBuffer &byte_buffer,  const bool get_replicas) {

  const char *pData = byte_buffer.data();
  int size = byte_buffer.size();

  const char *pCurrent = pData;
  int totalRead = 0;

  hbase::pb::RegionInfo ri;

  RegionDetails *region_details = new RegionDetails();

  while (totalRead < size) {
    DLOG(INFO) << "Sarting the loop";
    unsigned int *pSize = (unsigned int*) pCurrent;
    unsigned int cellSize = *pSize;
    CommonUtils::SwapByteOrder(cellSize);
    pCurrent = pCurrent + 4;
    totalRead += 4;

    pSize = (unsigned int*) pCurrent;
    unsigned int keyLength = *pSize;
    CommonUtils::SwapByteOrder(keyLength);
    pCurrent += 4;
    totalRead += 4;

    pSize = (unsigned int*) pCurrent;
    unsigned int valueLength = *pSize;
    CommonUtils::SwapByteOrder(valueLength);
    pCurrent += 4;
    totalRead += 4;

    std::unique_ptr<char[]> key_ptr(new char[keyLength]);
    char *key = key_ptr.get();
    std::memcpy(key, pCurrent, keyLength);
    pCurrent += keyLength;
    totalRead += keyLength;

    int row_offset = 2;

    unsigned short rowLength; //2 bytes
    char *pRow;
    unsigned char columnFamilyLength; //1 byte
    char *pColumnFamily;
    char *pColumnQualifier;
    char *keyType; //byte  1 byte

    char *pCellData = key;
    unsigned short *pRowLength = (unsigned short*) pCellData;
    rowLength = *pRowLength;
    CommonUtils::SwapByteOrder2Bytes(rowLength);
    pCellData += 2;

    std::unique_ptr<char[]> row_ptr(new char[rowLength + 1]);
    pRow = row_ptr.get();
    std::memcpy(pRow, pCellData, rowLength);
    pRow[rowLength] = '\0';
    region_details->SetRegionName(pRow);
    pCellData += rowLength;
    int family_length_offset = row_offset + rowLength;

    columnFamilyLength = *pCellData;
    pCellData += 1;
    std::unique_ptr<char[]> cf_ptr(new char[columnFamilyLength + 1]);
    pColumnFamily = cf_ptr.get();
    std::memcpy(pColumnFamily, pCellData, columnFamilyLength);
    pColumnFamily[columnFamilyLength] = '\0';
    pCellData += columnFamilyLength;

    int column_family_offset = family_length_offset + 1;
    int column_qualifier_offset = column_family_offset + columnFamilyLength;
    int timestamp_offset = keyLength - (8 + 1);

    int column_qualifier_length = timestamp_offset
        - column_qualifier_offset;
    std::unique_ptr<char[]> cq_ptr(new char[column_qualifier_length + 1]);
    pColumnQualifier = cq_ptr.get();
    std::memcpy(pColumnQualifier, pCellData, column_qualifier_length);
    pColumnQualifier[column_qualifier_length] = '\0';
    pCellData += column_qualifier_length;

    int timestamp_length = 8;
    Poco::UInt64 *pts = (Poco::UInt64*) pCellData;
    Poco::UInt64 ts = *pts;
    ts = Poco::ByteOrder::flipBytes(ts);
    pCellData += timestamp_length;

    int keyType_length = 1;
    std::unique_ptr<char[]> kt_ptr(new char[keyType_length + 1]);
    keyType = kt_ptr.get();
    std::memcpy(keyType, pCellData, keyType_length);
    keyType[keyType_length] = '\0';
    pCellData += keyType_length;

    std::unique_ptr<char[]> vl_ptr(new char[valueLength + 1]);
    char *value = vl_ptr.get();
    std::memcpy(value, pCurrent, valueLength);
    value[valueLength] = '\0';
    pCurrent += valueLength;
    totalRead += valueLength;

    if (std::strcmp(pColumnQualifier, "regioninfo") == 0) {

      char *pValue = value;
      char PB_MAGIC[4] = { 'P', 'B', 'U', 'F' };
      for (int i = 0; i < 4; i++) {
        if ((*pValue++) == PB_MAGIC[i]) {
          continue;
        } else {
          throw new std::string("Expecting a protobuf Region here");
        }
      }

      google::protobuf::io::ArrayInputStream arr(pValue, valueLength - 4);
      google::protobuf::io::CodedInputStream input(&arr);

      bool success = ri.ParseFromCodedStream(&input);
      region_details->SetRegionId(ri.region_id());
      region_details->SetStartKey(ri.start_key());
      region_details->SetEndKey(ri.end_key());
      region_details->SetOffline(ri.offline());
      region_details->SetSplit(ri.split());
      region_details->SetReplicaId(ri.replica_id());

      const hbase::pb::TableName &tn = ri.table_name();
      TableName *pTableName = TableName::CreateTableNameIfNecessary(
          tn.namespace_(), tn.qualifier());
      region_details->SetTableName(pTableName);
    } else if (std::strcmp(pColumnQualifier, "seqnumDuringOpen") == 0) {

    } else if (std::strcmp(pColumnQualifier, "serverstartcode") == 0) {
      Poco::UInt64 *pServer_start_code = (Poco::UInt64*) value;
      unsigned long server_start_code = *pServer_start_code;
      region_details->SetServerStartCode(
          Poco::ByteOrder::flipBytes(server_start_code));
      DLOG(INFO) << "Server start code: " << server_start_code;
    } else if ( (std::strcmp(pColumnQualifier, "server") == 0) ||
        (std::strncmp(pColumnQualifier, "server_", 7) == 0)
    ) {
      std::string lRegionIP = value;

      int counter = 0;
      const char *pIPAddress = lRegionIP.c_str();
      while (true) {
        if (*pIPAddress != ':') {
          counter++;
        } else {

          std::unique_ptr<char[]> ip_ptr(new char[counter + 1]);
          char *pIP = ip_ptr.get();
          std::memcpy(pIP, lRegionIP.c_str(), counter);
          pIP[counter] = '\0';

          if(!get_replicas)


            pIPAddress++;
          int numBytes = lRegionIP.length() - counter;

          std::unique_ptr<char[]> port_ptr(new char[numBytes + 1]);
          char *pPort = port_ptr.get();
          std::memcpy(pPort, pIPAddress, numBytes);
          pPort[numBytes] = '\0';
          unsigned int port = std::atol(pPort);
          if (!get_replicas)

            DLOG(INFO)  << "RegionServer IP: " << pIP ;
          DLOG(INFO) << "RegionServer Port: " << port ;
          if (get_replicas) {
            DLOG(INFO) << "Get Replicas";
            region_details->AddReplicaRegionServers(pIP, port);
          } else {
            DLOG(INFO) << "DO NOT Get Replicas";
            if ( (0 == region_details->GetRegionServerIpAddr().size()) && (0 == region_details->GetRegionServerPort()) ){
              region_details->SetRegionServerIpAddr(pIP);
              region_details->SetRegionServerPort(port);
            }
          }

          break;
        }
        pIPAddress += 1;
      }

    }
  }

  return region_details;
}

const std::string& RegionDetails::GetEndKey() const {
  return end_key_;
}

void RegionDetails::SetEndKey(const std::string& endKey) {
  end_key_ = endKey;
}

bool RegionDetails::IsOffline() const {
  return offline_;
}

void RegionDetails::SetOffline(bool offline) {
  offline_ = offline;
}

unsigned long RegionDetails::GetRegionId() const {
  return region_id_;
}

void RegionDetails::SetRegionId(unsigned long regionId) {
  region_id_ = regionId;
}

const std::string& RegionDetails::GetRegionName() const {
  return region_name_;
}

void RegionDetails::SetRegionName(const std::string &regionName) {
  region_name_ = regionName;
}

void RegionDetails::SetRegionName(const char *regionName) {
  region_name_ = regionName;
}

const std::string& RegionDetails::GetRegionServerIpAddr() const {
  return region_server_ip_addr_;
}

void RegionDetails::SetRegionServerIpAddr(
    const std::string& regionServerIpAddr) {
  region_server_ip_addr_ = regionServerIpAddr;
}

void RegionDetails::SetRegionServerIpAddr(const char *regionServerIpAddr) {
  region_server_ip_addr_ = regionServerIpAddr;
}

int RegionDetails::GetRegionServerPort() const {
  return region_server_port_;
}

void RegionDetails::SetRegionServerPort(int regionServerPort) {
  region_server_port_ = regionServerPort;
}

unsigned int RegionDetails::GetReplicaId() const {
  return replica_id_;
}

void RegionDetails::SetReplicaId(unsigned int replicaId) {
  replica_id_ = replicaId;
}

bool RegionDetails::IsSplit() const {
  return split_;
}

void RegionDetails::SetSplit(bool split) {
  split_ = split;
}

const std::string& RegionDetails::GetStartKey() const {
  return start_key_;
}

void RegionDetails::SetStartKey(const std::string &startKey) {
  start_key_ = startKey;
}

const TableName& RegionDetails::GetTableName() const {
  return *table_name_;
}

void RegionDetails::SetTableName(const TableName *tableName) {
  table_name_ = const_cast<TableName*> (tableName);
}

unsigned long RegionDetails::GetServerStartCode() const {
  return server_start_code_;
}

void RegionDetails::SetServerStartCode(unsigned long serverStartCode) {
  server_start_code_ = serverStartCode;
}

bool RegionDetails::operator < (const RegionDetails &rhs) const {
  if (this->start_key_ < rhs.start_key_)
    return true;

  if (this->start_key_ > rhs.start_key_)
    return false;

  if (this->end_key_ == rhs.end_key_) {
    return false;
  }

  if (this->end_key_ < rhs.end_key_)
    return true;

  if (this->end_key_ > rhs.end_key_)
    return false;

  return false;
}

void RegionDetails::GetCellsFromCellMetaBlock( const CellScanner &cell_scanner) {

  const char *cell_block = cell_scanner.GetData();
  int cell_size = cell_scanner.GetDataLength();

  int offset = 0;
  unsigned int cell_size_length;

  const char *pCurrent = cell_block;
  if (cell_size > 0) {
    while (cell_size !=offset) {

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
      delete cell;
    }

  }
}

void RegionDetails::AddReplicaRegionServers(const std::string &ip_address, const int &port) {

  REGION_SERVER_DETAILS rs_details;
  rs_details = std::make_pair(ip_address, port);

  this->region_server_details_.push_back(rs_details);

}

const std::vector<REGION_SERVER_DETAILS> &RegionDetails::GetReplicaRegionServers(){

  return this->region_server_details_;

}

