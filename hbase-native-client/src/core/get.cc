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

#include "get.h"
#include <glog/logging.h>
#include "exception.h"

Get::~Get() {

}

Get::Get(const BYTE_ARRAY &row)
    : max_versions_(1),
      cache_blocks_(true),
      store_limit_(-1),
      store_offset_(0),
      check_existence_only_(false),
      closest_row_before_(false),
      consistency_(CONSISTENCY::STRONG),
      tr_(TimeRange()) {

  Mutation::CheckRow(row);
  Bytes::CopyByteArray(row, row_, 0, row.size());
  this->family_map_.clear();
}

Get::Get(const Get &cget) {

  this->row_ = cget.row_;
  this->max_versions_ = cget.max_versions_;
  this->cache_blocks_ = cget.cache_blocks_;
  this->store_limit_ = cget.store_limit_;
  this->store_offset_ = cget.store_offset_;
  this->check_existence_only_ = cget.check_existence_only_;
  this->closest_row_before_ = cget.closest_row_before_;

  //TODO Need Copy Constructors for below 2 lines in
  this->consistency_ = cget.consistency_;
  this->tr_ = cget.tr_;
  this->family_map_ = cget.family_map_;
}

Get& Get::operator=(const Get &cget) {

  this->row_ = cget.row_;
  this->max_versions_ = cget.max_versions_;
  this->cache_blocks_ = cget.cache_blocks_;
  this->store_limit_ = cget.store_limit_;
  this->store_offset_ = cget.store_offset_;
  this->check_existence_only_ = cget.check_existence_only_;
  this->closest_row_before_ = cget.closest_row_before_;

  //TODO Need Copy Constructors for below 2 lines in
  this->consistency_ = cget.consistency_;
  this->tr_ = cget.tr_;
  this->family_map_ = cget.family_map_;
  return *this;
}

Get & Get::AddFamily(const BYTE_ARRAY &family) {

  bool family_found = false;
  for (auto& key_value : this->family_map_) {
    if (Bytes::Equals(key_value.first, family)) {
      family_found = true;
      // Remove any other set qualifiers
      key_value.second.clear();
      key_value.second.push_back(HBaseConstants::HConstants::EMPTY_BYTE_ARRAY);
      break;
    }
  }
  if (!family_found) {
    this->family_map_[family].push_back(
        HBaseConstants::HConstants::EMPTY_BYTE_ARRAY);
  }
  return *this;
}

Get & Get::AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier) {

  if (0 == qualifier.size()) {
    this->family_map_[family].push_back(
        HBaseConstants::HConstants::EMPTY_BYTE_ARRAY);
  } else {
    this->family_map_[family].push_back(qualifier);
  }

  return *this;
}
const BYTE_ARRAY &Get::GetRow() const {

  return this->row_;
}

const std::string Get::GetRowAsString() const {
  return Bytes::ToString(this->row_);
}

const CONSISTENCY &Get::GetConsistency() const {

  return this->consistency_;
}

Get &Get::SetConsistency(const CONSISTENCY &consistency) {

  this->consistency_ = consistency;
  return *this;
}

void Get::DisplayObj() {

  Bytes::DisplayBytes(this->row_);
  for (const auto& family_qual : this->family_map_) {
    std::string qualifiers("");
    for (const auto& qual : family_qual.second) {
      qualifiers +=
          ((qual.size() > 0) ? Bytes::ToString(qual) : "NO_QUALIFIER");
      qualifiers += ";";
    }
    DLOG(INFO) << Bytes::ToString(family_qual.first) << "[" << qualifiers
        << "]";
  }

}

bool Get::HasFamilies() {

  return !this->family_map_.empty();
}

const std::map<BYTE_ARRAY, std::vector<BYTE_ARRAY> > &Get::GetFamily() const {

  return this->family_map_;
}

const int& Get::GetMaxVersions() const {

  return this->max_versions_;
}

Get& Get::SetMaxVersions() {

  this->max_versions_ = std::numeric_limits<int>::max();
  return *this;
}

Get& Get::SetMaxVersions(const int &max_versions) {

  if (max_versions <= 0)
    throw HBaseException("max_versions must be positive");

  this->max_versions_ = max_versions;
  return *this;
}

const bool & Get::GetCacheBlocks() const {

  return this->cache_blocks_;
}

Get & Get::SetCacheBlocks(const bool &cache_blocks) {

  this->cache_blocks_ = cache_blocks;
  return *this;
}

const int& Get::GetMaxResultsPerColumnFamily() const {

  return this->store_limit_;
}

Get& Get::SetMaxResultsPerColumnFamily(const int &store_limit) {
  this->store_limit_ = store_limit;
  return *this;
}

const int Get::NumFamilies() {

  return this->family_map_.size();
}

Get& Get::SetTimeRange(const long &min_timestamp, const long &max_timestamp) {

  this->tr_ = TimeRange(min_timestamp, max_timestamp);
  return *this;
}

Get& Get::SetTimeStamp(const long &timestamp) {

  this->tr_ = TimeRange(timestamp, timestamp + 1);
  return *this;
}

const TimeRange& Get::GetTimeRange() const {

  return this->tr_;
}

int Get::GetAcl(BYTE_ARRAY &attr_value) {

  return Query::GetAcl(attr_value);
}

Get Get::SetAcl(const std::string &user, Permission &permissions) {

  Query::SetAcl(user, permissions);
  return *this;
}

Get Get::SetAcl(const std::map<std::string, Permission> &permissions) {

  Query::SetAcl(permissions);
  return *this;
}

Filter *Get::GetFilter() {

  return Query::GetFilter();
}

Get Get::SetFilter(const Filter &filter) {

  Query::SetFilter(filter);
  return *this;
}

