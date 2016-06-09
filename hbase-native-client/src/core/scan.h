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
#include "get.h"
#include "filter.h"
#include "bytes.h"
#include "consistency.h"
#include "time_range.h"

class Scan {
 public:
  Scan();
  ~Scan();

  Scan(byte *startRow, Filter *filter);
  Scan(byte *startRow);
  Scan(byte *startRow, byte *stopRow);
  Scan(Get &get);
  Scan(const Scan &scan);
  Scan AddFamily(const BYTE_ARRAY &family);
  Scan AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier);
  void SetReversed(bool &reversed);
  void SetStartRowVal(std::string &startRowVal);
  void SetStopRowVal(std::string &stopRpwVal);
  void SetSmall(bool &isSmall);
  void SetCaching(int &cache);
  Scan& SetConsistency(const CONSISTENCY &consistency);
  void SetCacheBlocks(bool &cacheBlocks);
  void SetAllowPartialResults(bool &allowPartialResults);
  void SetLoadColumnFamiliesOnDemandValue(bool &loadColumnFamiliesOnDemandValue);
  void SetMaxVersions(int &maxVersions);
  bool& IsReversed();
  std::string& GetStartRowVal();
  std::string& GetStopRowVal();
  bool& IsSmall();
  int& GetCaching();
  CONSISTENCY& GetConsistency();
  bool& GetCacheBlocks();
  bool& GetAllowPartialResults();
  bool& GetLoadColumnFamiliesOnDemandValue();
  int& GetMaxVersions();

  int& GetBatch();
  void SetMaxResultSize(long maxResultSize);
  int& GetMaxResultsPerColumnFamily();
  int& GetRowOffsetPerColumnFamily();
  long& GetMaxResultSize();
  bool IsGetScan();
  std::string& GetFilter();
  bool& IsAsyncPrefetch();
  const TimeRange& GetTimeRange();
  bool IsStartRowAndEqualsStopRow();
  const std::map<BYTE_ARRAY, std::vector<BYTE_ARRAY> >& GetFamilyMap();

  bool HasFamilies();
  bool IsScanMetricsEnabled();
  const int NumFamilies();
  Scan  &SetMaxResultsPerColumnFamily(const int &limit);
  Scan  &SetMaxVersions();
  Scan  &SetTimeRange(const long &minStamp, const long &maxStamp);
  Scan  &SetTimeStamp(const long &timestamp);

 private:
  void Init();
  std::string startRowVal_;
  std::string stopRowVal_;
  int maxVersions_;
  int batch_;
  int storeLimit_;
  int storeOffset_;
  int cache_;
  long maxResultSize_;
  bool cacheBlocks_;
  bool loadColumnFamiliesOnDemandValue_;
  bool reversed_;
  bool asyncPrefetch_;
  bool small_;
  bool allowPartialResults_;
  bool getScan_;
  CONSISTENCY consistency_;
  std::string filter_;
  TimeRange tr_;
  std::map<BYTE_ARRAY, std::vector<BYTE_ARRAY>> familyMap_;
};
