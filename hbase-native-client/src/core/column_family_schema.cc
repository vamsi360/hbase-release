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

#include "column_family_schema.h"

#include <glog/logging.h>

#include "configuration.h"
#include "exception.h"

const BYTE_TYPE ColumnFamilySchema::COLUMN_DESCRIPTOR_VERSION = static_cast<BYTE_TYPE>(11);
const std::string ColumnFamilySchema::COMPRESSION = "COMPRESSION";
const std::string ColumnFamilySchema::COMPRESSION_COMPACT = "COMPRESSION_COMPACT";
const std::string ColumnFamilySchema::ENCODE_ON_DISK =  "ENCODE_ON_DISK";
const std::string ColumnFamilySchema::DATA_BLOCK_ENCODING = "DATA_BLOCK_ENCODING";
const std::string ColumnFamilySchema::BLOCKCACHE = "BLOCKCACHE";
const std::string ColumnFamilySchema::CACHE_DATA_ON_WRITE = "CACHE_DATA_ON_WRITE";
const std::string ColumnFamilySchema::CACHE_INDEX_ON_WRITE = "CACHE_INDEX_ON_WRITE";
const std::string ColumnFamilySchema::CACHE_BLOOMS_ON_WRITE = "CACHE_BLOOMS_ON_WRITE";
const std::string ColumnFamilySchema::EVICT_BLOCKS_ON_CLOSE = "EVICT_BLOCKS_ON_CLOSE";
const std::string ColumnFamilySchema::CACHE_DATA_IN_L1 = "CACHE_DATA_IN_L1";
const std::string ColumnFamilySchema::PREFETCH_BLOCKS_ON_OPEN = "PREFETCH_BLOCKS_ON_OPEN";
const std::string ColumnFamilySchema::BLOCKSIZE = "BLOCKSIZE";
const std::string ColumnFamilySchema::LENGTH = "LENGTH";
const std::string ColumnFamilySchema::TTL = "TTL";
const std::string ColumnFamilySchema::BLOOMFILTER = "BLOOMFILTER";
const std::string ColumnFamilySchema::FOREVER = "FOREVER";
const std::string ColumnFamilySchema::REPLICATION_SCOPE = "REPLICATION_SCOPE";
const BYTE_ARRAY ColumnFamilySchema::REPLICATION_SCOPE_BYTES = Bytes::ToBytes(ColumnFamilySchema::REPLICATION_SCOPE);
const std::string ColumnFamilySchema::MIN_VERSIONS = "MIN_VERSIONS";
const std::string ColumnFamilySchema::KEEP_DELETED_CELLS = "KEEP_DELETED_CELLS";
const std::string ColumnFamilySchema::COMPRESS_TAGS = "COMPRESS_TAGS";
const std::string ColumnFamilySchema::ENCRYPTION = "ENCRYPTION";
const std::string ColumnFamilySchema::ENCRYPTION_KEY = "ENCRYPTION_KEY";
const std::string ColumnFamilySchema::IS_MOB = "IS_MOB";
const BYTE_ARRAY ColumnFamilySchema::IS_MOB_BYTES = Bytes::ToBytes(ColumnFamilySchema::IS_MOB);
const std::string ColumnFamilySchema::MOB_THRESHOLD = "MOB_THRESHOLD";
const BYTE_ARRAY ColumnFamilySchema::MOB_THRESHOLD_BYTES = Bytes::ToBytes(ColumnFamilySchema::MOB_THRESHOLD);
const long ColumnFamilySchema::DEFAULT_MOB_THRESHOLD = 100 * 1024; // 100k
const std::string ColumnFamilySchema::DFS_REPLICATION = "DFS_REPLICATION";
const short ColumnFamilySchema::DEFAULT_DFS_REPLICATION = 0;
const std::string ColumnFamilySchema::DEFAULT_COMPRESSION =
    HBaseConstants::EnumToString::ToString(HBaseConstants::COMPRESSION_ALGORITHM::NONE);
const bool ColumnFamilySchema::DEFAULT_ENCODE_ON_DISK = true;
/** Default data block encoding algorithm. */
const std::string ColumnFamilySchema::DEFAULT_DATA_BLOCK_ENCODING =
    HBaseConstants::EnumToString::ToString(HBaseConstants::DATA_BLOCK_ENCODING::NONE);
//TODO we need to get it from conf file. what is the defined path ???
const int ColumnFamilySchema::DEFAULT_VERSIONS = 1;    //HBaseConfiguration.create().getInt("hbase.column.max.version", 1);
const int ColumnFamilySchema::DEFAULT_MIN_VERSIONS = 0;
const bool ColumnFamilySchema::DEFAULT_IN_MEMORY = false;
const HBaseConstants::KEEP_DELETED_CELLS ColumnFamilySchema::DEFAULT_KEEP_DELETED = HBaseConstants::KEEP_DELETED_CELLS::FALSE;
const bool ColumnFamilySchema::DEFAULT_BLOCKCACHE = true;
const bool ColumnFamilySchema::DEFAULT_CACHE_DATA_ON_WRITE = false;
const bool ColumnFamilySchema::DEFAULT_CACHE_DATA_IN_L1 = false;
const bool ColumnFamilySchema::DEFAULT_CACHE_INDEX_ON_WRITE = false;
const int ColumnFamilySchema::DEFAULT_BLOCKSIZE = HBaseConstants::HConstants::DEFAULT_BLOCKSIZE;
const std::string ColumnFamilySchema::DEFAULT_BLOOMFILTER =
    HBaseConstants::EnumToString::ToString(HBaseConstants::BLOOM_TYPE::ROW);
const bool ColumnFamilySchema::DEFAULT_CACHE_BLOOMS_ON_WRITE = false;
const int ColumnFamilySchema::DEFAULT_TTL = HBaseConstants::HConstants::FOREVER;
const int ColumnFamilySchema::DEFAULT_REPLICATION_SCOPE = HBaseConstants::HConstants::REPLICATION_SCOPE_LOCAL;
const bool ColumnFamilySchema::DEFAULT_EVICT_BLOCKS_ON_CLOSE = false;
const bool ColumnFamilySchema::DEFAULT_COMPRESS_TAGS = true;
const bool ColumnFamilySchema::DEFAULT_PREFETCH_BLOCKS_ON_OPEN = false;
const int ColumnFamilySchema::UNINITIALIZED = -1;

const std::string ColumnFamilySchema::VERSIONS = HBaseConstants::HConstants::VERSIONS;//"VERSIONS";//
const std::string ColumnFamilySchema::IN_MEMORY = HBaseConstants::HConstants::IN_MEMORY;//"IN_MEMORY";

const std::map<std::string, std::string> ColumnFamilySchema::DEFAULT_VALUES = ColumnFamilySchema::PopulateDefaultValues();
const std::vector <Bytes> RESERVED_KEYWORDS = ColumnFamilySchema::PopulateStaticVals();

std::vector <Bytes> ColumnFamilySchema::PopulateStaticVals() {

  std::vector <Bytes> reserved_keywords;
  for (const auto &key_value : ColumnFamilySchema::DEFAULT_VALUES) {
    reserved_keywords.push_back(Bytes::ToBytes(key_value.first));
  }
  reserved_keywords.push_back(Bytes::ToBytes(ColumnFamilySchema::ENCRYPTION));
  reserved_keywords.push_back(Bytes::ToBytes(ColumnFamilySchema::ENCRYPTION_KEY));
  reserved_keywords.push_back(ColumnFamilySchema::IS_MOB_BYTES);
  reserved_keywords.push_back(ColumnFamilySchema::MOB_THRESHOLD_BYTES);

  return reserved_keywords;
}

std::map<std::string, std::string> ColumnFamilySchema::PopulateDefaultValues() {

  std::map<std::string, std::string> default_values;
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::BLOOMFILTER, ColumnFamilySchema::DEFAULT_BLOOMFILTER));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::REPLICATION_SCOPE, std::to_string(ColumnFamilySchema::DEFAULT_REPLICATION_SCOPE)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::VERSIONS, std::to_string(ColumnFamilySchema::DEFAULT_VERSIONS)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::MIN_VERSIONS, std::to_string(ColumnFamilySchema::DEFAULT_MIN_VERSIONS)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::COMPRESSION, ColumnFamilySchema::DEFAULT_COMPRESSION));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::TTL, std::to_string(ColumnFamilySchema::DEFAULT_TTL)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::BLOCKSIZE, std::to_string(ColumnFamilySchema::DEFAULT_BLOCKSIZE)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::IN_MEMORY, std::to_string(ColumnFamilySchema::DEFAULT_IN_MEMORY)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::BLOCKCACHE, std::to_string(ColumnFamilySchema::DEFAULT_BLOCKCACHE)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::KEEP_DELETED_CELLS, std::to_string(static_cast<BYTE_TYPE>(ColumnFamilySchema::DEFAULT_KEEP_DELETED))));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::DATA_BLOCK_ENCODING, ColumnFamilySchema::DEFAULT_DATA_BLOCK_ENCODING));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::CACHE_DATA_ON_WRITE, std::to_string(ColumnFamilySchema::DEFAULT_CACHE_DATA_ON_WRITE)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::CACHE_DATA_IN_L1, std::to_string(ColumnFamilySchema::DEFAULT_CACHE_DATA_IN_L1)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::CACHE_INDEX_ON_WRITE, std::to_string(ColumnFamilySchema::DEFAULT_CACHE_INDEX_ON_WRITE)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::CACHE_BLOOMS_ON_WRITE, std::to_string(ColumnFamilySchema::DEFAULT_CACHE_BLOOMS_ON_WRITE)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::EVICT_BLOCKS_ON_CLOSE, std::to_string(ColumnFamilySchema::DEFAULT_EVICT_BLOCKS_ON_CLOSE)));
  default_values.insert(std::pair<std::string, std::string>(ColumnFamilySchema::PREFETCH_BLOCKS_ON_OPEN, std::to_string(ColumnFamilySchema::DEFAULT_PREFETCH_BLOCKS_ON_OPEN)));
  return default_values;
}

ColumnFamilySchema::ColumnFamilySchema() {
  // TODO Auto-generated constructor stub

}

ColumnFamilySchema::ColumnFamilySchema(const std::string &family_name):ColumnFamilySchema(Bytes::ToBytes(family_name)) {

}


ColumnFamilySchema::ColumnFamilySchema(const BYTE_ARRAY &family_name) {

  if (IsLegalFamilyName(family_name)) {
    this->family_name_ = family_name;
    SetMaxVersions(ColumnFamilySchema::DEFAULT_VERSIONS);
    SetMinVersions(ColumnFamilySchema::DEFAULT_MIN_VERSIONS);
    SetKeepDeletedCells(ColumnFamilySchema::DEFAULT_KEEP_DELETED);
    SetInMemory(ColumnFamilySchema::DEFAULT_IN_MEMORY);
    SetBlockCacheEnabled(ColumnFamilySchema::DEFAULT_BLOCKCACHE);
    SetTimeToLive(ColumnFamilySchema::DEFAULT_TTL);
    SetCompressionType(HBaseConstants::COMPRESSION_ALGORITHM::NONE);
    SetDataBlockEncoding(HBaseConstants::DATA_BLOCK_ENCODING::NONE);
    SetBloomFilterType(HBaseConstants::BLOOM_TYPE::ROW);
    SetBlocksize(ColumnFamilySchema::DEFAULT_BLOCKSIZE);
    SetScope(ColumnFamilySchema::DEFAULT_REPLICATION_SCOPE);
  }
}

ColumnFamilySchema::~ColumnFamilySchema() {
  // TODO Auto-generated destructor stub
}

bool ColumnFamilySchema::IsLegalFamilyName(const BYTE_ARRAY &family_name) {

  if (0 == family_name.size())
    throw HBaseException("Family name can not be empty.\n");

  if ('.' == family_name[0]) {
    throw HBaseException("Family names cannot start with a period.\n");
  }

  for (unsigned int i = 0; i < family_name.size(); i++) {
    if (  (family_name[i] >= 0 && family_name[i] <= 31) || family_name[i] == ':' || family_name[i] == '\\' || family_name[i] == '/') {
      std::string throw_str("");
      throw_str += "Illegal character <";
      throw_str += family_name[i];
      throw_str += ">. Family names cannot contain control characters or colons\n";
      throw HBaseException(throw_str);
    }
  }
  BYTE_ARRAY recovered_edit = Bytes::ToBytes(HBaseConstants::HConstants::RECOVERED_EDITS_DIR);
  if (Bytes::Equals(recovered_edit, family_name)) {
    std::string throw_str("");
    throw_str += "Family name cannot be: ";
    throw_str += HBaseConstants::HConstants::RECOVERED_EDITS_DIR;
    throw_str += "\n";
    throw HBaseException(throw_str);
  }
  return true;
}

ColumnFamilySchema ColumnFamilySchema::SetMaxVersions(const int &max_versions) {

  if (max_versions <= 0) {
    // TODO: Allow maxVersion of 0 to be the way you say "Keep all versions".
    // Until there is support, consider 0 or < 0 -- a configuration error.
    throw HBaseException("Maximum versions must be positive");
  }
  if (max_versions < this->GetMinVersions()) {
    throw HBaseException("Maximum versions must be >= minimum versions");
  }
  this->SetValue(ColumnFamilySchema::VERSIONS, std::to_string(max_versions));
  cached_max_versions_ = max_versions;
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::SetMinVersions(const int &min_versions) {

  this->SetValue(ColumnFamilySchema::MIN_VERSIONS, std::to_string(min_versions));
  return *this;

}

ColumnFamilySchema ColumnFamilySchema::SetKeepDeletedCells(const HBaseConstants::KEEP_DELETED_CELLS &keep_deleted_cells) {

  this->SetValue(ColumnFamilySchema::KEEP_DELETED_CELLS, HBaseConstants::EnumToString::ToString(keep_deleted_cells));
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::SetInMemory(const bool &set_in_memory) {

  this->SetValue(ColumnFamilySchema::IN_MEMORY, std::to_string(set_in_memory));
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::SetBlockCacheEnabled(const bool &set_block_cache) {

  this->SetValue(ColumnFamilySchema::BLOCKCACHE, std::to_string(set_block_cache));
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::SetTimeToLive(const int &ttl) {

  this->SetValue(ColumnFamilySchema::TTL, std::to_string(ttl));
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::SetCompressionType(const HBaseConstants::COMPRESSION_ALGORITHM &compression_algorithm) {

  this->SetValue(ColumnFamilySchema::COMPRESSION, HBaseConstants::EnumToString::ToString(compression_algorithm));
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::SetDataBlockEncoding(const HBaseConstants::DATA_BLOCK_ENCODING &data_block_encoding) {

  this->SetValue(ColumnFamilySchema::DATA_BLOCK_ENCODING, HBaseConstants::EnumToString::ToString(data_block_encoding));
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::SetBloomFilterType(const HBaseConstants::BLOOM_TYPE &bloom_type) {

  this->SetValue(ColumnFamilySchema::BLOOMFILTER, HBaseConstants::EnumToString::ToString(bloom_type));
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::SetBlocksize(const int &default_blocksize) {

  this->SetValue(ColumnFamilySchema::BLOCKSIZE, std::to_string(default_blocksize));
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::SetScope(const int &replication_scope) {

  this->SetValue(ColumnFamilySchema::REPLICATION_SCOPE, std::to_string(replication_scope));
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::SetValue(const std::string &key, const std::string &value) {

  if (0 == value.size()) {
    this->RemoveValue(key);
  } else {

    this->SetValue(Bytes::ToBytes(key), Bytes::ToBytes(value));
  }
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::SetValue(const BYTE_ARRAY &key, const BYTE_ARRAY &value) {

  BYTE_ARRAY max_versions_bytes;
  Bytes::ToBytes(ColumnFamilySchema::VERSIONS, max_versions_bytes);
  if (Bytes::Equals(key, max_versions_bytes)) {
    this->cached_max_versions_ = ColumnFamilySchema::UNINITIALIZED;
  }
  Bytes key_to_set(key);
  bool found = false;
  for (std::map<Bytes*, Bytes*>::iterator itr_bytes = this->column_metadata_values_.begin(); itr_bytes != this->column_metadata_values_.end(); ++itr_bytes) {
    if (Bytes::Equals(key, itr_bytes->first->Get())) {
      found = true;
      itr_bytes->second->Set(value);
      break;
    }
  }
  if (!found) {
    this->column_metadata_values_.insert(std::pair<Bytes*, Bytes*>(new Bytes(key), new Bytes(value)));
  }

  return *this;
}

ColumnFamilySchema ColumnFamilySchema::RemoveValue(const std::string &key){

  this->RemoveValue(Bytes::ToBytes(key));
  return *this;
}

ColumnFamilySchema ColumnFamilySchema::RemoveValue(const BYTE_ARRAY &key){

  std::map<Bytes*, Bytes*>::iterator itr_bytes = this->column_metadata_values_.begin();
  for ( ; itr_bytes != this->column_metadata_values_.end(); ++itr_bytes) {
    if (Bytes::Equals(key, itr_bytes->first->Get())) {
      delete (itr_bytes->second);
      this->column_metadata_values_.erase(itr_bytes);
      break;
    }
  }
  return *this;
}


/**
 * @return The minimum number of versions to keep.
 */
int ColumnFamilySchema::GetMinVersions() {

  int min_versions;
  std::string value = GetValue(ColumnFamilySchema::MIN_VERSIONS);
  min_versions = (0 == value.size()) ? 0 : atoi(value.c_str());

  return min_versions;
}

/**
 * @param key The key.
 * @return The value as a string.
 */
std::string ColumnFamilySchema::GetValue(const std::string &key) {

  std::string value_str("");
  BYTE_ARRAY value = GetValue(Bytes::ToBytes(key));
  if (value.size() > 0)
    Bytes::ToString(value, value_str);
  return value_str;
}

/**
 * @param key The key.
 * @return The value.
 */
BYTE_ARRAY ColumnFamilySchema::GetValue(const BYTE_ARRAY &key) {

  BYTE_ARRAY set_bytes;
  for (const auto &key_value : this->column_metadata_values_) {
    if (Bytes::Equals(key, key_value.first->Get())) {
      set_bytes = key_value.first->Get();
      break;
    }
  }

  return set_bytes;
}

const BYTE_ARRAY &ColumnFamilySchema::GetName() const{
  return this->family_name_;
}

const std::string ColumnFamilySchema::GetNameAsString() const{
  return Bytes::ToString(this->family_name_);
}

const std::map<Bytes*, Bytes*> &ColumnFamilySchema::GetValues() const{
  return this->column_metadata_values_;
}

const std::map<std::string, std::string> &ColumnFamilySchema::GetConfiguration() const{
  return this->column_configuration_;
}

ColumnFamilySchema ColumnFamilySchema::SetConfiguration(const std::string &key, const std::string &value){

  if (0 == value.size()) {
    this->RemoveConfiguration(key);
  } else {
    this->column_configuration_[key] = value;
  }

  return *this;
}

ColumnFamilySchema ColumnFamilySchema::RemoveConfiguration(const std::string &key){

  try {

    this->column_configuration_.erase(this->column_configuration_.at(key));

  } catch(const std::out_of_range &oor) {

    DLOG(INFO) << "Cant remove Key " << key << "from column configuration";
  }

  return *this;
}
