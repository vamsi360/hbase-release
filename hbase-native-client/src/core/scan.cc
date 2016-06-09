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

#include "scan.h"
#include <glog/logging.h>

Scan::Scan() {
  Init();
  this->familyMap_.clear();
  DLOG(INFO) << "Scan 1 constructor ";

}

void Scan::Init() {
  startRowVal_ = "";
  stopRowVal_ = "";
  maxVersions_ = 1;
  batch_ = -1;
  storeLimit_ = -1;
  storeOffset_ = 0;
  cache_ = -1;
  maxResultSize_ = -1;
  cacheBlocks_ = true;
  loadColumnFamiliesOnDemandValue_ = NULL;
  reversed_ = false;
  asyncPrefetch_ = NULL;
  small_ = false;
  allowPartialResults_ = false;
  getScan_ = false;
}

Scan::~Scan() {
  DLOG(INFO) << "Scan  destructor ";
}

Scan::Scan(const Scan &scan){

  this->startRowVal_ = scan.startRowVal_;
  this->stopRowVal_ = scan.stopRowVal_;
  this->maxVersions_ = scan.maxVersions_;
  this->batch_ = scan.batch_;
  this->storeLimit_ = scan.storeLimit_;
  this->storeOffset_ = scan.storeOffset_;
  this->cache_ = scan.cache_;
  this->maxResultSize_ = scan.maxResultSize_;
  this->cacheBlocks_ = scan.cacheBlocks_;
  this->getScan_ = scan.getScan_;
  this->filter_ = scan.filter_; // clone?
  this->loadColumnFamiliesOnDemandValue_ =
      scan.loadColumnFamiliesOnDemandValue_;
  this->consistency_ = scan.consistency_;
  this->reversed_ = scan.reversed_;
  this->asyncPrefetch_ = scan.asyncPrefetch_;
  this->small_ = scan.small_;
  this->allowPartialResults_ = scan.allowPartialResults_;
  this->tr_ = scan.tr_;
  this->familyMap_ = scan.familyMap_;
  DLOG(INFO) << "Scan 2 constructor ";
}

Scan::Scan(byte *startRowkey, Filter *filter) {
  Init();
  this->startRowVal_ = startRowkey;
  DLOG(INFO) << "Scan 3 constructor ";

}

Scan::Scan(byte *startRowkey) {
  Init();
  DLOG(INFO) << "Scan 4 constructor ";
}

Scan::Scan(byte *startRow, byte *stopRow) {
  Init();
  this->startRowVal_ = startRow;
  this->stopRowVal_ = stopRow;
  //if the startRow and stopRow both are empty, it is not a Get
  this->getScan_ = false;//isStartRowAndEqualsStopRow();
  DLOG(INFO) << "Scan 5 constructor ";

}

Scan::Scan(Get &get){
  Init();
  this->cacheBlocks_ = get.GetCacheBlocks();
  this->maxVersions_ = get.GetMaxVersions();
  this->storeLimit_ = get.GetMaxResultsPerColumnFamily();
  this->familyMap_ = get.GetFamily();
  this->getScan_ = true;
  this->asyncPrefetch_ = false;
  DLOG(INFO) << "Scan 6 constructor ";
}

Scan Scan::AddFamily(const BYTE_ARRAY &family){
  bool family_found = false;
  for (auto& key_value : this->familyMap_) {
    if (Bytes::Equals(key_value.first, family)) {
      family_found = true;
      // Remove any other set qualifiers
      key_value.second.clear();
      break;
    }
  }
  if(!family_found){
    this->familyMap_[family].push_back(HBaseConstants::HConstants::EMPTY_BYTE_ARRAY);
  }
  return *this;
}

Scan Scan::AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier){

  if (0 == qualifier.size()) {
    this->familyMap_[family].push_back(HBaseConstants::HConstants::EMPTY_BYTE_ARRAY);
  } else {
    this->familyMap_[family].push_back(qualifier);
  }

  return *this;
}

void Scan::SetReversed(bool &reversed) {
  reversed_ = reversed;
}

void Scan::SetSmall(bool &isSmall) {
  small_ = isSmall;
}

void Scan::SetCaching(int &cache) {
  cache_ = cache;
}

Scan& Scan::SetConsistency(const CONSISTENCY& consistency) {
  consistency_ = consistency;
  return *this;
}

void Scan::SetCacheBlocks(bool& cacheBlocks) {
  cacheBlocks_ = cacheBlocks;
}

void Scan::SetAllowPartialResults(bool& allowPartialResults) {
  allowPartialResults_ = allowPartialResults;
}

void Scan::SetLoadColumnFamiliesOnDemandValue(bool& loadColumnFamiliesOnDemandValue) {
  loadColumnFamiliesOnDemandValue_ = loadColumnFamiliesOnDemandValue;
}

void Scan::SetMaxVersions(int& maxVersions) {
  maxVersions_ = maxVersions;
}

bool& Scan::IsReversed() {
  return reversed_;
}

bool& Scan::IsSmall() {
  return small_;
}

int& Scan::GetCaching() {
  return cache_;
}

CONSISTENCY& Scan::GetConsistency() {
  return consistency_;
}

bool& Scan::GetCacheBlocks() {
  return cacheBlocks_;
}

bool& Scan::GetAllowPartialResults() {
  return allowPartialResults_;
}

bool& Scan::GetLoadColumnFamiliesOnDemandValue() {
  return loadColumnFamiliesOnDemandValue_;
}

int& Scan::GetMaxVersions() {
  return maxVersions_;
}

std::string& Scan::GetStartRowVal() {
  return startRowVal_;
}

std::string&  Scan::GetStopRowVal() {
  return stopRowVal_;
}

void Scan::SetStopRowVal(std::string& stoprowval) {
  stopRowVal_ = stoprowval;
}

void Scan::SetStartRowVal(std::string& startrowval) {
  startRowVal_ = startrowval;
}

int& Scan::GetBatch() {
  return batch_;
}

void Scan::SetMaxResultSize(long maxResultSize) {
  maxResultSize_ = maxResultSize;
}
int& Scan::GetMaxResultsPerColumnFamily() {
  return storeLimit_;
}
int& Scan::GetRowOffsetPerColumnFamily() {
  return storeOffset_;
}
long& Scan::GetMaxResultSize() {
  return maxResultSize_;
}
bool  Scan::IsGetScan() {
  if(getScan_ || IsStartRowAndEqualsStopRow())
    return true;
  else
    return false;
}
std::string& Scan::GetFilter() {
  return filter_;
}
bool& Scan::IsAsyncPrefetch() {
  return asyncPrefetch_;
}
const TimeRange& Scan::GetTimeRange() {
  return tr_;
}
bool Scan::IsStartRowAndEqualsStopRow() {
  if((!this->startRowVal_.empty()) && (this->startRowVal_.length() > 0) && (this->startRowVal_== this->stopRowVal_))
    return true;
  else
    return false ;
}
const std::map <BYTE_ARRAY, std::vector<BYTE_ARRAY> > &Scan::GetFamilyMap() {
  return this->familyMap_;
}

bool Scan::HasFamilies(){
  return !this->familyMap_.empty();
}

bool Scan::IsScanMetricsEnabled() {

}

const int Scan::NumFamilies() {
  return this->familyMap_.size();
}

Scan& Scan::SetMaxResultsPerColumnFamily(const int &limit) {
  this->storeLimit_ = limit;
  return *this;
}

Scan& Scan::SetMaxVersions() {
  this->maxVersions_ = std::numeric_limits< int >::max();
  return *this;
}

Scan& Scan::SetTimeRange(const long& minStamp, const long& maxStamp) {
  this->tr_ = TimeRange(minStamp, maxStamp);
  return *this;
}

Scan& Scan::SetTimeStamp(const long& timestamp) {
  this->tr_ = TimeRange(timestamp, timestamp+1);
  return *this;
}

