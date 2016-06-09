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
 */

#include "put.h"

#include <cstdlib>
#include <sstream>
#include <glog/logging.h>
#include "exception.h"

Put::~Put() {}

Put::Put(const BYTE_ARRAY &row): Put(row, HBaseConstants::HConstants::LATEST_TIMESTAMP){

}

Put::Put(const BYTE_ARRAY &row, const long &timestamp): Put(row, 0, row.size(), timestamp){

}

Put::Put(const BYTE_ARRAY &row, const int &row_offset, const int &row_length): Put(row, row_offset, row_length, HBaseConstants::HConstants::LATEST_TIMESTAMP){

}

Put::Put(const BYTE_ARRAY &row, const int &row_offset, const int &row_length, const long &ts){

  if (ts < 0) {
    throw HBaseException("Timestamp cannot be negative.\n");
  }
  Mutation::CheckRow(row);
  this->row_ = row;
  this->ts_ = ts;
}

Put::Put(const Put& cPut){

  this->row_ = cPut.row_;
  this->ts_ = cPut.ts_;
  this->family_map_ = cPut.family_map_;

}

Put& Put::operator= (const Put &cPut){

  this->row_ = cPut.row_;
  this->ts_ = cPut.ts_;
  this->family_map_ = cPut.family_map_;
  return *this;

}

Put& Put::Add(const KeyValue &key_value){

  BYTE_ARRAY row;
  Bytes::CopyByteArray(key_value.GetRowArray(), row, key_value.GetRowOffset(), key_value.GetRowLength());
  BYTE_ARRAY family;
  Bytes::CopyByteArray(key_value.GetFamilyArray(), row, key_value.GetFamilyOffset(), key_value.GetFamilyLength());
  if(!Bytes::Equals(this->row_, row)){
    std::stringstream str_error ;
    str_error.str("");
    str_error << "Key Value being inserted for [" << Bytes::ToString(row)
    <<  "]. Put constructed for [ " << Bytes::ToString(this->row_) << "]\n";
    throw HBaseException(str_error.str());
  }
  this->family_map_[family].push_back(key_value);
  return *this;
}

Put & Put::AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier, const BYTE_ARRAY &value){
  return AddColumn(family, qualifier, this->ts_, value);
}

Put & Put::AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
    const long &ts, const BYTE_ARRAY &value){

  if (ts < 0)
    throw HBaseException("Timestamp cannot be negative.\n");

  KeyValue *key_value = nullptr;
  key_value = CreatePutKeyValue(family, qualifier, ts, value);
  this->family_map_[family].push_back(*key_value);
  delete key_value;
  return *this;
}

int Put::GetAcl(BYTE_ARRAY &attr_value){

  return Mutation::GetAcl(attr_value);
}

Put & Put::SetAcl(const std::string &user, Permission perms) {

  Mutation::SetAcl(user, perms);
  return *this;
}

Put & Put::SetAcl(const std::map<std::string, Permission> &perms) {

  Mutation::SetAcl(perms);
  return *this;
}

const long Put::GetTtl(BYTE_ARRAY &attr_value) {

  return Mutation::GetTtl(attr_value);
}

Put & Put::SetTtl(const long &ttl) {

  Mutation::SetTtl(ttl);
  return *this;
}

int Put::Get(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier, std::vector<Cell> &cellList) {

  return 0;
}
