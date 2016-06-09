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

#include "meta_cache.h"
#include <glog/logging.h>

MetaCache::MetaCache() {

}

MetaCache::~MetaCache() {
  std::map<TableName, RegionList> cache_;

  for (std::map<TableName ,RegionList>::iterator it=cache_.begin();
      it!=cache_.end(); ++it)	{
    RegionList *region_list = &it->second;
    for (RegionList::iterator rlit = region_list->begin();
        rlit != region_list->end(); ++rlit) {
      region_list->erase(rlit);
    }

    cache_.erase(it);
  }
}

RegionDetails* MetaCache::LookupRegion(const TableName &table_name,
    const std::string &row_key) {

  std::map<TableName, RegionList>::iterator it;

  it = cache_.find(table_name);
  if (it == cache_.end()) {
    return nullptr;
  }

  DLOG(INFO) << "Found Details in cache for " << it->first.GetName();
  RegionList &region_list = it->second;
  for (auto rd : region_list) {
    DLOG(INFO) << "row_key:[" << row_key << "]; rd->GetStartKey():[" << rd->GetStartKey() << "]; rd->GetEndKey():[" << rd->GetEndKey() << "];";
    if (row_key >= rd->GetStartKey() &&
        row_key < rd->GetEndKey()) {
      return rd;
    }
  }
  return nullptr;
}

void MetaCache::CacheRegion(const TableName &table_name,
    RegionDetails *region_details) {

  if(region_details->GetStartKey().size() > 0 && region_details->GetEndKey().size() > 0){
    std::map<TableName, RegionList>::iterator it;

    it = cache_.find(table_name);

    if (it != cache_.end()) {
      it->second.insert(region_details);
    } else {
      RegionList region_list;
      region_list.insert(region_details);
      cache_.insert(std::make_pair(table_name, region_list));
    }
  }else{
    DLOG(INFO) << "Skipping region server cachng as StartKey & EndKey sizes are 0";
  }
}

bool MetaCache::InvalidateCache(const TableName &table_name){

  bool cache_cleared = true;
  std::map<TableName, RegionList>::iterator it;

  it = cache_.find(table_name);

  if (it != cache_.end()) {
    it->second.clear();
  } else {
    cache_cleared = false;
  }

  return cache_cleared;
}

