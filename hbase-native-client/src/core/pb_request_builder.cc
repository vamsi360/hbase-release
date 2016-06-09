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

#include "pb_request_builder.h"

#include <glog/logging.h>
#include "family_filter.h"
#include "binary_comparator.h"
#include "compare_filter.h"

ProtoBufRequestBuilder::ProtoBufRequestBuilder() {

}

ProtoBufRequestBuilder::~ProtoBufRequestBuilder() {

}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreateScanRequest(
    const std::string &regionName, Scan &scan, int numberOfRows,
    bool closeScanner) {

  hbase::pb::ScanRequest *scanRequest = new hbase::pb::ScanRequest();

  scanRequest->set_number_of_rows(numberOfRows);
  scanRequest->set_close_scanner(closeScanner);

  hbase::pb::RegionSpecifier *regionSpecifier =
      new hbase::pb::RegionSpecifier();
  hbase::pb::RegionSpecifier_RegionSpecifierType rsType =
      hbase::pb::RegionSpecifier_RegionSpecifierType_REGION_NAME;

  regionSpecifier->set_value(regionName);
  regionSpecifier->set_type(rsType);

  scanRequest->set_allocated_region(regionSpecifier);

  hbase::pb::Scan *scanPb = new hbase::pb::Scan();

  const std::map<BYTE_ARRAY, std::vector<BYTE_ARRAY>> &family_map = scan
      .GetFamilyMap();
  for (const auto& key_value : family_map) {
    hbase::pb::Column *colObj = scanPb->add_column();

    std::string col_str;
    Bytes::ToString(key_value.first, col_str);
    colObj->set_family(col_str);
    for (const auto& qual : key_value.second) {
      std::string qual_str;
      Bytes::ToString(qual, qual_str);
      colObj->add_qualifier(qual_str);
    }
  }

  scanPb->set_cache_blocks(scan.GetCacheBlocks());

  if (scan.IsSmall()) {
    scanPb->set_small(scan.IsSmall());
  }

  scanPb->set_max_versions(scan.GetMaxVersions());

  if (scan.IsReversed()) {
    scanPb->set_reversed(scan.IsReversed());
  }

  if (scan.GetCaching() > 0) {
    scanPb->set_caching(scan.GetCaching());
  }
  if (scan.GetStartRowVal() != "") {
    scanPb->set_start_row(scan.GetStartRowVal());
  }
  if (scan.GetStopRowVal() != "") {
    scanPb->set_stop_row(scan.GetStopRowVal());
  }

  scanPb->set_max_result_size(2097152);  //2MB

  scanRequest->set_allocated_scan(scanPb);
  scanRequest->set_client_handles_partials(true);
  scanRequest->set_client_handles_heartbeats(true);
  scanRequest->set_track_scan_metrics(false);

  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(scanRequest);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;
}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreateScanRequestForScannerId(
    long scannerId, int numberOfRows, bool closeScanner, bool trackMetrics) {

  hbase::pb::ScanRequest *scanRequest = new hbase::pb::ScanRequest();

  scanRequest->set_number_of_rows(numberOfRows);
  scanRequest->set_close_scanner(closeScanner);
  scanRequest->set_scanner_id(scannerId);
  scanRequest->set_client_handles_partials(true);
  scanRequest->set_client_handles_heartbeats(true);
  scanRequest->set_track_scan_metrics(trackMetrics);

  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(scanRequest);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;
}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreateScanRequestForScannerId(
    long scannerId, int numberOfRows, bool closeScanner, bool trackMetrics,
    long nextCallSeq, bool renew) {

  hbase::pb::ScanRequest *scanRequest = new hbase::pb::ScanRequest();

  google::protobuf::uint64 scnr_id =
      static_cast<google::protobuf::uint64>(scannerId);

  scanRequest->set_number_of_rows(numberOfRows);
  scanRequest->set_close_scanner(closeScanner);
  scanRequest->set_scanner_id(scnr_id);
  scanRequest->set_client_handles_partials(true);
  scanRequest->set_client_handles_heartbeats(true);
  scanRequest->set_track_scan_metrics(trackMetrics);
  scanRequest->set_next_call_seq(nextCallSeq);
  scanRequest->set_renew(renew);

  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(scanRequest);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;
}

void ProtoBufRequestBuilder::CreateRegionSpecifier(
    const std::string &regionName, hbase::pb::RegionSpecifier &regObj) {

  regObj.set_type(hbase::pb::RegionSpecifier_RegionSpecifierType_REGION_NAME);
  regObj.set_value(regionName);
}

void ProtoBufRequestBuilder::AddMutationFamilies(
    hbase::pb::MutationProto &mutate, const FAMILY_MAP & family_map) {

  for (const auto &family : family_map) {
    hbase::pb::MutationProto_ColumnValue *colObj = mutate.add_column_value();
    std::string col_str;
    Bytes::ToString(family.first, col_str);
    colObj->set_family(col_str);
    for (const auto &key_value : family.second) {
      hbase::pb::MutationProto_ColumnValue_QualifierValue *colQual = colObj
          ->add_qualifier_value();
      BYTE_ARRAY qualifier;
      BYTE_ARRAY value;

      if (key_value.GetQualifierLength() > 0) {
        Bytes::CopyByteArray(key_value.GetQualifierArray(), qualifier,
                             key_value.GetQualifierOffset(),
                             key_value.GetQualifierLength());
        colQual->set_qualifier(Bytes::ToString(qualifier));
        qualifier.clear();
      }

      if (mutate.has_mutate_type()) {
        switch (mutate.mutate_type()) {
          case hbase::pb::MutationProto_MutationType_PUT: {
            if (key_value.GetValueLength() > 0) {
              Bytes::CopyByteArray(key_value.GetValueArray(), value,
                                   key_value.GetValueOffset(),
                                   key_value.GetValueLength());
              colQual->set_value(Bytes::ToString(value));
              value.clear();
            }
            break;
          }
          case hbase::pb::MutationProto_MutationType_DELETE: {
            colQual->set_delete_type(
                ProtoBufRequestBuilder::ToDeleteType(key_value.GetTypeByte()));
            break;
          }
          default:
            break;
        }
      }
      colQual->set_timestamp(key_value.GetTimestamp());
    }  //for multiple Qualifiers
  }
}

void ProtoBufRequestBuilder::CreateMutationMessage(
    const Put &put, hbase::pb::MutationProto &mutate) {

  DLOG(INFO) << mutate.DebugString() << std::endl;

  mutate.set_row(put.GetRowAsString());
  DLOG(INFO) << mutate.DebugString() << std::endl;

  mutate.set_mutate_type(hbase::pb::MutationProto_MutationType_PUT);
  mutate.set_durability(hbase::pb::MutationProto_Durability_USE_DEFAULT);
  mutate.set_timestamp(HBaseConstants::HConstants::LATEST_TIMESTAMP);

  const FAMILY_MAP & family_map = put.GetFamilyMap();
  ProtoBufRequestBuilder::AddMutationFamilies(mutate, family_map);
  DLOG(INFO) << mutate.DebugString() << std::endl;
}

void ProtoBufRequestBuilder::CreateMutationMessage(
    const Delete &delete_obj, hbase::pb::MutationProto &mutate) {

  mutate.set_row(delete_obj.GetRowAsString());
  mutate.set_mutate_type(hbase::pb::MutationProto_MutationType_DELETE);
  mutate.set_durability(hbase::pb::MutationProto_Durability_USE_DEFAULT);
  mutate.set_timestamp(HBaseConstants::HConstants::LATEST_TIMESTAMP);

  const FAMILY_MAP & family_map = delete_obj.GetFamilyMap();
  ProtoBufRequestBuilder::AddMutationFamilies(mutate, family_map);

}

void ProtoBufRequestBuilder::CreateGetMessage(const Get &get,
                                              hbase::pb::Get &getObj) {

  const std::map<BYTE_ARRAY, std::vector<BYTE_ARRAY>> &family_map = get
      .GetFamily();
  for (const auto& key_value : family_map) {
    hbase::pb::Column *colObj = getObj.add_column();
    std::string col_str;
    Bytes::ToString(key_value.first, col_str);
    colObj->set_family(col_str);
    for (const auto& qual : key_value.second) {
      if (qual.size() > 0) {
        std::string qual_str;
        Bytes::ToString(qual, qual_str);
        colObj->add_qualifier(qual_str);
      }
    }
  }
  getObj.set_row(get.GetRowAsString());
  getObj.set_max_versions(get.GetMaxVersions());
  getObj.set_cache_blocks(get.GetCacheBlocks());
  if (-1 != get.GetMaxResultsPerColumnFamily())
    getObj.set_store_limit(get.GetMaxResultsPerColumnFamily());

  hbase::pb::Consistency pb_consistency;
  switch (get.GetConsistency()) {
    case CONSISTENCY::STRONG: {
      pb_consistency = hbase::pb::Consistency::STRONG;
      break;
    }
    case CONSISTENCY::TIMELINE: {
      pb_consistency = hbase::pb::Consistency::TIMELINE;
      break;
    }
    default:
      pb_consistency = hbase::pb::Consistency::STRONG;
  }
  getObj.set_consistency(pb_consistency);

  if (!get.GetTimeRange().GetAllTime()) {
    hbase::pb::TimeRange *time_range = new hbase::pb::TimeRange();
    time_range->set_from(get.GetTimeRange().GetMin());
    time_range->set_to(get.GetTimeRange().GetMax());
    getObj.set_allocated_time_range(time_range);
  }
}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreateGetRequest(
    const Get &get, const std::string &regionName) {

  hbase::pb::RegionSpecifier *regionSpecifier =
      new hbase::pb::RegionSpecifier();
  ProtoBufRequestBuilder::CreateRegionSpecifier(regionName, *regionSpecifier);

  BYTE_ARRAY row_bytes;
  row_bytes = get.GetRow();
  std::string row_str;
  Bytes::ToString(row_bytes, row_str);
  hbase::pb::Get *getObj = new hbase::pb::Get();
  ProtoBufRequestBuilder::CreateGetMessage(get, *getObj);

  hbase::pb::GetRequest *req = new hbase::pb::GetRequest();
  req->set_allocated_get(getObj);
  req->set_allocated_region(regionSpecifier);

  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(req);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;
}

std::map<std::string, hbase::pb::MultiRequest*> ProtoBufRequestBuilder::CreateGetRequest(
    const std::map<std::string, std::vector<Get *>> &regioninfo_rowinfo) {

  std::map<std::string, hbase::pb::MultiRequest*> multi_requests;
  for (auto regioninfo_rowdetails : regioninfo_rowinfo) {
    hbase::pb::MultiRequest *req = new hbase::pb::MultiRequest();
    hbase::pb::RegionSpecifier *regionSpecifier =
        new hbase::pb::RegionSpecifier();
    ProtoBufRequestBuilder::CreateRegionSpecifier(regioninfo_rowdetails.first,
                                                  *regionSpecifier);
    hbase::pb::RegionAction *regAction = req->add_regionaction();
    regAction->set_allocated_region(regionSpecifier);
    unsigned int actionNum = 0;
    for (actionNum = 0; actionNum < regioninfo_rowdetails.second.size();
        actionNum++) {
      Get *get = regioninfo_rowdetails.second[actionNum];
      //get->DisplayObj();
      hbase::pb::Get *getObj = new hbase::pb::Get();
      ProtoBufRequestBuilder::CreateGetMessage(*get, *getObj);
      hbase::pb::Action *action = regAction->add_action();
      action->set_index(actionNum);
      action->set_allocated_get(getObj);
    }
    multi_requests.insert(
        std::pair<std::string, hbase::pb::MultiRequest*>(
            regioninfo_rowdetails.first, req));
  }

  return multi_requests;
}

std::map<std::string, hbase::pb::MultiRequest*> ProtoBufRequestBuilder::CreateGetRequest(
    const std::map<std::string, std::vector<Get *>> &regioninfo_rowinfo,
    const std::map<std::string, std::map<int, int>> &failed_result_or_exception_retries,
    const int &threshold) {

  std::map<std::string, hbase::pb::MultiRequest*> multi_requests;
  if (failed_result_or_exception_retries.size() > 0) {
    for (const auto &regioninfo_rowdetails : regioninfo_rowinfo) {
      const auto &retry_counts_per_op = failed_result_or_exception_retries.at(
          regioninfo_rowdetails.first);
      hbase::pb::MultiRequest *req = new hbase::pb::MultiRequest();
      hbase::pb::RegionSpecifier *regionSpecifier =
          new hbase::pb::RegionSpecifier();
      ProtoBufRequestBuilder::CreateRegionSpecifier(regioninfo_rowdetails.first,
                                                    *regionSpecifier);
      hbase::pb::RegionAction *regAction = req->add_regionaction();
      //regAction->set_atomic(true);
      regAction->set_allocated_region(regionSpecifier);
      unsigned int actionNum = 0;
      for (actionNum = 0; actionNum < regioninfo_rowdetails.second.size();
          actionNum++) {
        if (retry_counts_per_op.at(actionNum) > threshold)
          continue;
        Get *get = regioninfo_rowdetails.second[actionNum];
        hbase::pb::Get *getObj = new hbase::pb::Get();
        ProtoBufRequestBuilder::CreateGetMessage(*get, *getObj);

        hbase::pb::Action *action = regAction->add_action();
        action->set_index(actionNum);
        action->set_allocated_get(getObj);
      }
      if (req->regionaction(0).action_size() > 0)
        multi_requests.insert(
            std::pair<std::string, hbase::pb::MultiRequest*>(
                regioninfo_rowdetails.first, req));
    }
  }

  return multi_requests;
}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreatePutRequest(
    const Put &put, const std::string &regionName) {

  hbase::pb::MultiRequest *req = new hbase::pb::MultiRequest();

  hbase::pb::RegionSpecifier *regionSpecifier =
      new hbase::pb::RegionSpecifier();
  ProtoBufRequestBuilder::CreateRegionSpecifier(regionName, *regionSpecifier);
  hbase::pb::RegionAction *regAction = req->add_regionaction();

  regAction->set_allocated_region(regionSpecifier);
  int actionNum = 0;
  hbase::pb::MutationProto *mutate = new hbase::pb::MutationProto();
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << req->ByteSize() << "]: " << req->DebugString() << std::endl;
  ProtoBufRequestBuilder::CreateMutationMessage(put, *mutate);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << req->ByteSize() << "]: " << req->DebugString() << std::endl;

  hbase::pb::Action *action = regAction->add_action();
  action->set_index(actionNum);
  action->set_allocated_mutation(mutate);

  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(req);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;
}

std::map<std::string, hbase::pb::MultiRequest*> ProtoBufRequestBuilder::CreatePutRequest(
    const std::map<std::string, std::vector<Put *>> &regioninfo_rowinfo) {

  std::map<std::string, hbase::pb::MultiRequest*> multi_requests;
  for (auto regioninfo_rowdetails : regioninfo_rowinfo) {
    hbase::pb::MultiRequest *req = new hbase::pb::MultiRequest();

    hbase::pb::RegionSpecifier *regionSpecifier =
        new hbase::pb::RegionSpecifier();
    ProtoBufRequestBuilder::CreateRegionSpecifier(regioninfo_rowdetails.first,
                                                  *regionSpecifier);
    hbase::pb::RegionAction *regAction = req->add_regionaction();

    regAction->set_allocated_region(regionSpecifier);
    unsigned int actionNum = 0;
    for (actionNum = 0; actionNum < regioninfo_rowdetails.second.size();
        actionNum++) {
      if (nullptr != regioninfo_rowdetails.second[actionNum]) {
        Put *put = regioninfo_rowdetails.second[actionNum];
        hbase::pb::MutationProto *mutate = new hbase::pb::MutationProto();
        ProtoBufRequestBuilder::CreateMutationMessage(*put, *mutate);
        hbase::pb::Action *action = regAction->add_action();
        action->set_index(actionNum);
        action->set_allocated_mutation(mutate);
      }
    }
    multi_requests.insert(
        std::pair<std::string, hbase::pb::MultiRequest*>(
            regioninfo_rowdetails.first, req));
  }

  return multi_requests;
}

std::map<std::string, hbase::pb::MultiRequest*> ProtoBufRequestBuilder::CreatePutRequest(
    const std::map<std::string, std::vector<Put *>> &regioninfo_rowinfo,
    const std::map<std::string, std::map<int, int>> &failed_result_or_exception_retries,
    const int &threshold) {

  std::map<std::string, hbase::pb::MultiRequest*> multi_requests;
  for (const auto regioninfo_rowdetails : regioninfo_rowinfo) {
    const auto &retry_counts_per_op = failed_result_or_exception_retries.at(
        regioninfo_rowdetails.first);
    hbase::pb::MultiRequest *req = new hbase::pb::MultiRequest();
    hbase::pb::RegionSpecifier *regionSpecifier =
        new hbase::pb::RegionSpecifier();
    ProtoBufRequestBuilder::CreateRegionSpecifier(regioninfo_rowdetails.first,
                                                  *regionSpecifier);
    hbase::pb::RegionAction *regAction = req->add_regionaction();

    regAction->set_allocated_region(regionSpecifier);
    unsigned int actionNum = 0;
    for (actionNum = 0; actionNum < regioninfo_rowdetails.second.size();
        actionNum++) {
      if (retry_counts_per_op.at(actionNum) > threshold)
        continue;
      Put *put = regioninfo_rowdetails.second[actionNum];
      hbase::pb::MutationProto *mutate = new hbase::pb::MutationProto();
      ProtoBufRequestBuilder::CreateMutationMessage(*put, *mutate);
      hbase::pb::Action *action = regAction->add_action();
      action->set_index(actionNum);
      action->set_allocated_mutation(mutate);
    }
    if (req->regionaction(0).action_size() > 0)
      multi_requests.insert(
          std::pair<std::string, hbase::pb::MultiRequest*>(
              regioninfo_rowdetails.first, req));
  }

  return multi_requests;
}

hbase::pb::MutationProto_DeleteType ProtoBufRequestBuilder::ToDeleteType(
    const BYTE_TYPE &type) {
  switch (static_cast<KeyValue::KEY_TYPE>(type)) {
    case KeyValue::KEY_TYPE::Delete:
      return hbase::pb::MutationProto_DeleteType_DELETE_ONE_VERSION;
    case KeyValue::KEY_TYPE::DeleteColumn:
      return hbase::pb::MutationProto_DeleteType_DELETE_MULTIPLE_VERSIONS;
    case KeyValue::KEY_TYPE::DeleteFamily:
      return hbase::pb::MutationProto_DeleteType_DELETE_FAMILY;
    case KeyValue::KEY_TYPE::DeleteFamilyVersion:
      return hbase::pb::MutationProto_DeleteType_DELETE_FAMILY_VERSION;
    default:
      return hbase::pb::MutationProto_DeleteType_DELETE_ONE_VERSION;
  }
}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreateDeleteRequest(
    const Delete &deleteObj, const std::string &regionName) {

  hbase::pb::RegionSpecifier *regionSpecifier =
      new hbase::pb::RegionSpecifier();
  ProtoBufRequestBuilder::CreateRegionSpecifier(regionName, *regionSpecifier);

  hbase::pb::MutationProto *mutationProto = new hbase::pb::MutationProto();
  ProtoBufRequestBuilder::CreateMutationMessage(deleteObj, *mutationProto);

  hbase::pb::MutateRequest *req = new hbase::pb::MutateRequest();
  req->set_allocated_region(regionSpecifier);
  req->set_allocated_mutation(mutationProto);

  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(req);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;
}

std::map<std::string, hbase::pb::MultiRequest*> ProtoBufRequestBuilder::CreateDeleteRequest(
    const std::map<std::string, std::vector<Delete *>> &regioninfo_rowinfo) {

  std::map<std::string, hbase::pb::MultiRequest*> multi_requests;
  for (const auto regioninfo_rowdetails : regioninfo_rowinfo) {

    hbase::pb::MultiRequest *req = new hbase::pb::MultiRequest();
    hbase::pb::RegionSpecifier *regionSpecifier =
        new hbase::pb::RegionSpecifier();
    ProtoBufRequestBuilder::CreateRegionSpecifier(regioninfo_rowdetails.first,
                                                  *regionSpecifier);
    hbase::pb::RegionAction *regAction = req->add_regionaction();
    regAction->set_allocated_region(regionSpecifier);
    unsigned int actionNum = 0;
    for (actionNum = 0; actionNum < regioninfo_rowdetails.second.size();
        actionNum++) {
      if (nullptr != regioninfo_rowdetails.second[actionNum]) {
        Delete *delete_ptr = regioninfo_rowdetails.second[actionNum];

        hbase::pb::MutationProto *mutate = new hbase::pb::MutationProto();
        ProtoBufRequestBuilder::CreateMutationMessage(*delete_ptr, *mutate);
        hbase::pb::Action *action = regAction->add_action();
        action->set_index(actionNum);
        action->set_allocated_mutation(mutate);
      }
    }
    multi_requests.insert(
        std::pair<std::string, hbase::pb::MultiRequest*>(
            regioninfo_rowdetails.first, req));
  }

  return multi_requests;
}

std::map<std::string, hbase::pb::MultiRequest*> ProtoBufRequestBuilder::CreateDeleteRequest(
    const std::map<std::string, std::vector<Delete *>> &regioninfo_rowinfo,
    const std::map<std::string, std::map<int, int>> &failed_result_or_exception_retries,
    const int &threshold) {

  std::map<std::string, hbase::pb::MultiRequest*> multi_requests;
  for (const auto regioninfo_rowdetails : regioninfo_rowinfo) {
    const auto &retry_counts_per_op = failed_result_or_exception_retries.at(
        regioninfo_rowdetails.first);
    hbase::pb::MultiRequest *req = new hbase::pb::MultiRequest();
    hbase::pb::RegionSpecifier *regionSpecifier =
        new hbase::pb::RegionSpecifier();
    ProtoBufRequestBuilder::CreateRegionSpecifier(regioninfo_rowdetails.first,
                                                  *regionSpecifier);
    hbase::pb::RegionAction *regAction = req->add_regionaction();

    regAction->set_allocated_region(regionSpecifier);
    unsigned int actionNum = 0;
    for (actionNum = 0; actionNum < regioninfo_rowdetails.second.size();
        actionNum++) {
      if (retry_counts_per_op.at(actionNum) > threshold)
        continue;
      Delete *delete_ptr = regioninfo_rowdetails.second[actionNum];
      hbase::pb::MutationProto *mutate = new hbase::pb::MutationProto();
      ProtoBufRequestBuilder::CreateMutationMessage(*delete_ptr, *mutate);
      hbase::pb::Action *action = regAction->add_action();
      action->set_index(actionNum);
      action->set_allocated_mutation(mutate);
    }
    if (req->regionaction(0).action_size() > 0)
      multi_requests.insert(
          std::pair<std::string, hbase::pb::MultiRequest*>(
              regioninfo_rowdetails.first, req));
  }

  return multi_requests;
}

void ProtoBufRequestBuilder::CreateTableName(const std::string &tableName,
                                             hbase::pb::TableName &tableObj) {

  std::string nameSpaceValue("");
  std::string tableNameValue("");
  GetTableAndNamespace(tableName, nameSpaceValue, tableNameValue);

  tableObj.set_namespace_(nameSpaceValue);
  tableObj.set_qualifier(tableNameValue);
}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreateEnableTableRequest(
    const std::string &table) {

  hbase::pb::TableName *tableName = new hbase::pb::TableName();
  ProtoBufRequestBuilder::CreateTableName(table, *tableName);

  hbase::pb::EnableTableRequest *req = new hbase::pb::EnableTableRequest();
  req->set_allocated_table_name(tableName);

  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(req);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;
}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreateDisableTableRequest(
    const std::string &table) {

  hbase::pb::TableName *tableName = new hbase::pb::TableName();
  ProtoBufRequestBuilder::CreateTableName(table, *tableName);

  hbase::pb::DisableTableRequest *req = new hbase::pb::DisableTableRequest();
  req->set_allocated_table_name(tableName);

  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(req);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;
}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreateProcedureRequest(
    const int & procedureId) {

  hbase::pb::GetProcedureResultRequest *req =
      new hbase::pb::GetProcedureResultRequest();
  req->set_proc_id(procedureId);

  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(req);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;
}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreateTableRequest(
    const TableSchema &table_schema) {

  hbase::pb::TableName *tableName = new hbase::pb::TableName();
  const TableName &table_name = table_schema.GetTableName();
  ProtoBufRequestBuilder::CreateTableName(table_name.GetName(), *tableName);

  hbase::pb::TableSchema *tabSchema = new hbase::pb::TableSchema();

  tabSchema->set_allocated_table_name(tableName);

  const std::map<Bytes*, Bytes*> &table_metadata = table_schema.GetMetadata();
  for (const auto & table_metadata_values : table_metadata) {
    hbase::pb::BytesBytesPair *tableAttr = tabSchema->add_attributes();
    tableAttr->set_first(Bytes::ToString(table_metadata_values.first->Get()));
    tableAttr->set_second(Bytes::ToString(table_metadata_values.second->Get()));
  }

  const std::map<BYTE_ARRAY, ColumnFamilySchema> &family_map = table_schema
      .GetFamily();
  for (const auto & col_family : family_map) {
    hbase::pb::ColumnFamilySchema *colFamSchema =
        tabSchema->add_column_families();
    colFamSchema->set_name(col_family.second.GetNameAsString());

    const std::map<Bytes*, Bytes*> &value_map = col_family.second.GetValues();
    for (const auto & col_family_values : value_map) {
      hbase::pb::BytesBytesPair *colFamAttr = colFamSchema->add_attributes();
      colFamAttr->set_first(Bytes::ToString(col_family_values.first->Get()));
      colFamAttr->set_second(Bytes::ToString(col_family_values.second->Get()));
    }

    const std::map<std::string, std::string> &config_map = col_family.second
        .GetConfiguration();
    for (const auto & col_family_config : config_map) {
      hbase::pb::NameStringPair *colFamConfig =
          colFamSchema->add_configuration();
      colFamConfig->set_name(col_family_config.first);
      colFamConfig->set_value(col_family_config.second);
    }
  }

  const std::map<std::string, std::string> &table_config_map = table_schema
      .GetConfiguration();
  for (const auto & table_config : table_config_map) {
    hbase::pb::NameStringPair *tableConfig = tabSchema->add_configuration();
    tableConfig->set_name(table_config.first);
    tableConfig->set_value(table_config.second);
  }

  hbase::pb::CreateTableRequest *req = new hbase::pb::CreateTableRequest();
  req->set_allocated_table_schema(tabSchema);
  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(req);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;
}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreateDeleteTableRequest(
    const std::string &table) {

  hbase::pb::TableName *tableName = new hbase::pb::TableName();
  ProtoBufRequestBuilder::CreateTableName(table, *tableName);

  hbase::pb::DeleteTableRequest *req = new hbase::pb::DeleteTableRequest();
  req->set_allocated_table_name(tableName);

  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(req);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;

}

std::unique_ptr<google::protobuf::Message> ProtoBufRequestBuilder::CreateListTablesRequest(
    const std::string &regEx, const bool &includeSysTables) {

  hbase::pb::GetTableDescriptorsRequest *req =
      new hbase::pb::GetTableDescriptorsRequest();
  if (regEx.length() > 0) {
    std::string *regExTables = new std::string(regEx);
    req->set_allocated_regex(regExTables);
  }
  req->set_include_sys_tables(includeSysTables);

  google::protobuf::Message *message =
      dynamic_cast<google::protobuf::Message *>(req);
  std::unique_ptr < google::protobuf::Message > ptr(message);
  DLOG(INFO) << "[" << __LINE__ << ":" << __func__ << "] DBG: Request:["
      << ptr->ByteSize() << "]: " << ptr->DebugString();
  return ptr;
}
