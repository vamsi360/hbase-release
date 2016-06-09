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

#include "delete.h"
#include "exception.h"

Delete::Delete(const BYTE_ARRAY &row): Delete(row, HBaseConstants::HConstants::LATEST_TIMESTAMP) {

}

Delete::Delete(const BYTE_ARRAY &row, const long &timestamp): Delete(row, 0, row.size(), timestamp) {

}

Delete::Delete(const BYTE_ARRAY &row, const int &row_offset, const int &row_length):
            Delete(row, row_offset, row_length, HBaseConstants::HConstants::LATEST_TIMESTAMP) {

}

Delete::Delete(const BYTE_ARRAY &row, const int &row_offset, const int &row_length, const long &ts) {

  if (ts < 0) {
    throw HBaseException("Timestamp cannot be negative.");
  }
  Mutation::CheckRow(row);
  this->row_ = row;
}

Delete::Delete(const Delete &cDelete) {

  this->row_ = cDelete.row_;
  this->ts_ = cDelete.ts_;
  this->family_map_ = cDelete.family_map_;

}

Delete& Delete::operator= (const Delete &cDelete) {

  this->row_ = cDelete.row_;
  this->ts_ = cDelete.ts_;
  this->family_map_ = cDelete.family_map_;
  return *this;

}

Delete::~Delete() {}

Delete &Delete::AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier) {
  return AddColumn(family, qualifier, this->ts_);
}

Delete &Delete::AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier, const long &timestamp) {

  if (timestamp < 0)
    throw HBaseException("Timestamp cannot be negative.");

  KeyValue *key_value = nullptr;
  key_value = new KeyValue(this->row_, family, qualifier, timestamp, static_cast<BYTE_TYPE>(KeyValue::KEY_TYPE::Delete));
  this->family_map_[family].push_back(*key_value);
  delete key_value;
  this->Display();
  return *this;
}

Delete& Delete::AddColumns(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier) {

  return AddColumns( family,  qualifier,this->ts_);
}

Delete& Delete::AddColumns(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier, const long &timestamp) {

  if (timestamp < 0)
    throw HBaseException("Timestamp cannot be negative.");

  KeyValue *key_value = nullptr;
  key_value = new KeyValue(this->row_, family, qualifier, timestamp, static_cast<BYTE_TYPE>(KeyValue::KEY_TYPE::DeleteColumn));
  this->family_map_[family].push_back(*key_value);
  delete key_value;
  return *this;
}

Delete& Delete::AddFamily(const BYTE_ARRAY &family) {

  return AddFamily( family,this->ts_);
}

Delete& Delete::AddFamily(const BYTE_ARRAY &family, const long &timestamp) {

  if (timestamp < 0)
    throw HBaseException("Timestamp cannot be negative.");

  KeyValue *key_value = nullptr;
  key_value = new KeyValue(this->row_, family, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY,
      timestamp, static_cast<BYTE_TYPE>(KeyValue::KEY_TYPE::DeleteFamily));
  this->family_map_[family].push_back(*key_value);
  delete key_value;
  return *this;
}

Delete& Delete::AddFamilyVersion(const BYTE_ARRAY &family, const long &timestamp) {

  if (timestamp < 0)
    throw HBaseException("Timestamp cannot be negative.");

  KeyValue *key_value = nullptr;
  key_value = new KeyValue(this->row_, family, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY,
      timestamp, static_cast<BYTE_TYPE>(KeyValue::KEY_TYPE::DeleteFamilyVersion));
  this->family_map_[family].push_back(*key_value);
  delete key_value;
  return *this;
}
