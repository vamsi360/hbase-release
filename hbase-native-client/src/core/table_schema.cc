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

#include "table_schema.h"

#include <glog/logging.h>
#include "exception.h"


const std::string TableSchema::SPLIT_POLICY = "SPLIT_POLICY";
const std::string TableSchema::OWNER = "OWNER";
const std::string TableSchema::MAX_FILESIZE = "MAX_FILESIZE";
const std::string TableSchema::READONLY = "READONLY";
const std::string TableSchema::COMPACTION_ENABLED = "COMPACTION_ENABLED";
const std::string TableSchema::MEMSTORE_FLUSHSIZE = "MEMSTORE_FLUSHSIZE";
const std::string TableSchema::FLUSH_POLICY = "FLUSH_POLICY";
const std::string TableSchema::IS_ROOT = "IS_ROOT";
const std::string TableSchema::IS_META = "IS_META";
const std::string TableSchema::DEFERRED_LOG_FLUSH = "DEFERRED_LOG_FLUSH";
const std::string TableSchema::DURABILITY = "DURABILITY";
const std::string TableSchema::REGION_REPLICATION = "REGION_REPLICATION";
const std::string TableSchema::REGION_MEMSTORE_REPLICATION = "REGION_MEMSTORE_REPLICATION";
const std::string TableSchema::NORMALIZATION_ENABLED = "NORMALIZATION_ENABLED";

const bool TableSchema::DEFAULT_READONLY = false;
const bool TableSchema::DEFAULT_COMPACTION_ENABLED = true;
const bool TableSchema::DEFAULT_NORMALIZATION_ENABLED = false;
const long TableSchema::DEFAULT_MEMSTORE_FLUSH_SIZE = 1024*1024*128L;
const int TableSchema::DEFAULT_REGION_REPLICATION = 1;
const bool TableSchema::DEFAULT_REGION_MEMSTORE_REPLICATION = true;
const HBaseConstants::DURABILITY_TYPE TableSchema::DEFAULT_DURABLITY = HBaseConstants::DURABILITY_TYPE::USE_DEFAULT;

const BYTE_ARRAY TableSchema::OWNER_KEY = Bytes::ToBytes(TableSchema::OWNER);
const BYTE_ARRAY TableSchema::MAX_FILESIZE_KEY = Bytes::ToBytes(TableSchema::MAX_FILESIZE);
const BYTE_ARRAY TableSchema::READONLY_KEY = Bytes::ToBytes(TableSchema::READONLY);
const BYTE_ARRAY TableSchema::COMPACTION_ENABLED_KEY = Bytes::ToBytes(TableSchema::COMPACTION_ENABLED);
const BYTE_ARRAY TableSchema::MEMSTORE_FLUSHSIZE_KEY = Bytes::ToBytes(TableSchema::MEMSTORE_FLUSHSIZE);
const BYTE_ARRAY TableSchema::IS_ROOT_KEY = Bytes::ToBytes(TableSchema::IS_ROOT);
const BYTE_ARRAY TableSchema::IS_META_KEY = Bytes::ToBytes(TableSchema::IS_META);
const BYTE_ARRAY TableSchema::DEFERRED_LOG_FLUSH_KEY = Bytes::ToBytes(TableSchema::DEFERRED_LOG_FLUSH);
const BYTE_ARRAY TableSchema::DURABILITY_KEY = Bytes::ToBytes(TableSchema::DURABILITY);
const BYTE_ARRAY TableSchema::REGION_REPLICATION_KEY = Bytes::ToBytes(TableSchema::REGION_REPLICATION);
const BYTE_ARRAY TableSchema::REGION_MEMSTORE_REPLICATION_KEY = Bytes::ToBytes(TableSchema::REGION_MEMSTORE_REPLICATION);
const BYTE_ARRAY TableSchema::NORMALIZATION_ENABLED_KEY = Bytes::ToBytes(TableSchema::NORMALIZATION_ENABLED);

const BYTE_ARRAY TableSchema::FALSE = Bytes::ToBytes(false);
const BYTE_ARRAY TableSchema::TRUE = Bytes::ToBytes(true);
const bool TableSchema::DEFAULT_DEFERRED_LOG_FLUSH = false;

const std::map<std::string, std::string> TableSchema::DEFAULT_VALUES = TableSchema::PopulateDefaultValues();
const std::vector <Bytes> RESERVED_KEYWORDS = TableSchema::PopulateStaticVals();

std::vector <Bytes> TableSchema::PopulateStaticVals(){

  std::vector <Bytes> reserved_keywords;
  for(const auto &key_value : TableSchema::DEFAULT_VALUES){
    reserved_keywords.push_back(Bytes::ToBytes(key_value.first));
  }

  reserved_keywords.push_back(TableSchema::IS_ROOT_KEY);
  reserved_keywords.push_back(TableSchema::IS_META_KEY);

  return reserved_keywords;
}

std::map<std::string, std::string> TableSchema::PopulateDefaultValues() {

  std::map<std::string, std::string> default_values;
  default_values.insert(std::pair<std::string, std::string>(TableSchema::MAX_FILESIZE, std::to_string(HBaseConstants::HConstants::DEFAULT_MAX_FILE_SIZE) ));
  default_values.insert(std::pair<std::string, std::string>(TableSchema::READONLY, std::to_string(TableSchema::DEFAULT_READONLY)));
  default_values.insert(std::pair<std::string, std::string>(TableSchema::MEMSTORE_FLUSHSIZE, std::to_string(TableSchema::DEFAULT_MEMSTORE_FLUSH_SIZE)));
  default_values.insert(std::pair<std::string, std::string>(TableSchema::DURABILITY, HBaseConstants::EnumToString::ToString(HBaseConstants::DURABILITY_TYPE::USE_DEFAULT)));
  default_values.insert(std::pair<std::string, std::string>(TableSchema::REGION_REPLICATION, std::to_string(TableSchema::DEFAULT_REGION_REPLICATION)));
  default_values.insert(std::pair<std::string, std::string>(TableSchema::NORMALIZATION_ENABLED, std::to_string(TableSchema::DEFAULT_NORMALIZATION_ENABLED)));
  return default_values;
}

TableSchema::TableSchema() {
  // TODO Auto-generated constructor stub


}

TableSchema::TableSchema(TableName *table_name):table_name_(table_name) {

}

TableSchema::TableSchema(const TableSchema &ctable_schema) {

  this->table_name_ = ctable_schema.table_name_;
  this->table_metadata_values_ = ctable_schema.table_metadata_values_;
  this->table_configuration_ = ctable_schema.table_configuration_;
  this->families_ = ctable_schema.families_;

}

TableSchema& TableSchema::operator= (const TableSchema &ctable_schema) {

  this->table_name_ = ctable_schema.table_name_;
  this->table_metadata_values_ = ctable_schema.table_metadata_values_;
  this->table_configuration_ = ctable_schema.table_configuration_;
  this->families_ = ctable_schema.families_;

  return *this;

}

TableSchema::~TableSchema() {
  // TODO Auto-generated destructor stub

}


TableSchema TableSchema::AddFamily(const ColumnFamilySchema &family) {

  if (family.GetName().size() <= 0) {
    throw HBaseException("Family name cannot be null or empty");
  }
  for(const auto &key_value : this->families_){
    if(Bytes::Equals(key_value.first, family.GetName())){
      std::string str_error("");
      str_error += "Family '";
      str_error += family.GetNameAsString();
      str_error += "' already exists so cannot be added\n";
      throw HBaseException(str_error);
    }
  }
  this->families_.insert(std::pair<BYTE_ARRAY, ColumnFamilySchema>(family.GetName(), family));
  return *this;
}

const std::map<BYTE_ARRAY, ColumnFamilySchema> &TableSchema::GetFamily() const{

  return this->families_;
}

const TableName &TableSchema::GetTableName() const{

  return *(this->table_name_);
}

const std::map<Bytes*, Bytes*> &TableSchema::GetMetadata() const{
  return this->table_metadata_values_;
}

const std::map<std::string, std::string> &TableSchema::GetConfiguration() const{
  return this->table_configuration_;
}

TableSchema TableSchema::SetValue(const std::string &key, const std::string &value) {

  if(0 == value.size()){
    this->RemoveValue(key);
  }else{

    this->SetValue(Bytes::ToBytes(key), Bytes::ToBytes(value));
  }
  return *this;
}

TableSchema TableSchema::SetValue(const BYTE_ARRAY &key, const BYTE_ARRAY &value) {

  Bytes key_to_set(key);
  bool found = false;
  for(std::map<Bytes*, Bytes*>::iterator itr_bytes = this->table_metadata_values_.begin(); itr_bytes != this->table_metadata_values_.end(); ++itr_bytes){
    if(Bytes::Equals(key, itr_bytes->first->Get())){
      found = true;
      itr_bytes->second->Set(value);
      break;
    }
  }
  if(!found){
    Bytes *key_to_set = new Bytes(key);
    Bytes *value_to_set = new Bytes(value);
    this->table_metadata_values_.insert(std::pair<Bytes*, Bytes*>(key_to_set, value_to_set));
  }

  return *this;
}

TableSchema TableSchema::RemoveValue(const std::string &key){

  this->RemoveValue(Bytes::ToBytes(key));
  return *this;
}

TableSchema TableSchema::RemoveValue(const BYTE_ARRAY &key){

  std::map<Bytes*, Bytes*>::iterator itr_bytes = this->table_metadata_values_.begin();
  for( ; itr_bytes != this->table_metadata_values_.end(); ++itr_bytes){
    if(Bytes::Equals(key, itr_bytes->first->Get())){
      delete(itr_bytes->second);
      this->table_metadata_values_.erase(itr_bytes);
      break;
    }
  }

  return *this;
}


TableSchema TableSchema::SetConfiguration(const std::string &key, const std::string &value){

  if(0 == value.size()){
    this->RemoveConfiguration(key);
  }else{
    this->table_configuration_[key] = value;
  }

  return *this;
}

TableSchema TableSchema::RemoveConfiguration(const std::string &key){

  try {

    this->table_configuration_.erase(this->table_configuration_.at(key));

  }catch(const std::out_of_range &oor){

    LOG(ERROR)  << "Cant remove Key " << key << "from column configuration" ;
  }

  return *this;
}

