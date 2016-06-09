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

#include <cstring>

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "bytes.h"
#include "consistency.h"
#include "filter.h"
//#include "hconstants.h"
#include "mutation.h"
#include "permission.h"
#include "query.h"
#include "return_codes.h"
#include "time_range.h"

class Get : public Query {

 public:
  Get(const BYTE_ARRAY &row);
  Get(const Get &cget);
  Get& operator=(const Get &cget);
  virtual ~Get();
  const int& GetMaxVersions() const;
  Get& SetMaxVersions();
  Get& SetMaxVersions(const int&);
  const bool& GetCacheBlocks() const;
  Get& SetCacheBlocks(const bool&);
  const int& GetMaxResultsPerColumnFamily() const;
  Get& SetMaxResultsPerColumnFamily(const int &storeLimit);
  void DisplayObj();
  const std::map<std::string, std::vector<std::string> > &GetFamilyStr();
  const std::map<BYTE_ARRAY, std::vector<BYTE_ARRAY> > &GetFamily() const;
  const int NumFamilies();
  const TimeRange & GetTimeRange() const;
  Get& SetTimeRange(const long &minTimeStamp, const long &maxTimeStamp);
  Get& SetTimeStamp(const long &timestamp);
  Get& AddFamily(const BYTE_ARRAY &family);
  Get& AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier);
  const BYTE_ARRAY &GetRow() const;
  const std::string GetRowAsString() const;
  bool HasFamilies();
  const CONSISTENCY& GetConsistency() const;
  Get &SetConsistency(const CONSISTENCY&);
  Filter *GetFilter();
  Get SetFilter(const Filter &filter);
  int GetAcl(BYTE_ARRAY &attr_value);
  Get SetAcl(const std::map<std::string, Permission> &permissions);
  Get SetAcl(const std::string &user, Permission &permissions);

 private:
  BYTE_ARRAY row_;
  int max_versions_;
  bool cache_blocks_;
  int store_limit_;
  int store_offset_;
  bool check_existence_only_;
  bool closest_row_before_;
  std::map<BYTE_ARRAY, std::vector<BYTE_ARRAY>> family_map_;
  CONSISTENCY consistency_;
  TimeRange tr_;
};
