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

#include "cell.h"
#include <glog/logging.h>
#include <Poco/ByteOrder.h>
#include "utils.h"

Cell::~Cell() {
  if (nullptr != key_value_)
    delete key_value_;
}

Cell::Cell(const std::string &row, const std::string &family, const std::string &qualifier,
    const long &timestamp, const std::string &value, const std::string &tags, const CellType::cellType &cellType):
                                               row_(row), family_(family), qualifier_(qualifier), timestamp_(timestamp), value_(value), tags_(tags),
                                               cellType_(cellType), key_value_(nullptr) {
  this->qualifier_ = (0 == this->qualifier_.length() ? "" : this->qualifier_);
  this->value_ = (0 == this->value_.length() ? "" : this->value_);

  key_value_ = new KeyValue( Bytes::ToBytes(row), Bytes::ToBytes(family),
      (0 == this->qualifier_.length() ? HBaseConstants::HConstants::EMPTY_BYTE_ARRAY : Bytes::ToBytes(qualifier)),
      timestamp, static_cast<BYTE_TYPE>(cellType),
      (0 == this->value_.length() ? HBaseConstants::HConstants::EMPTY_BYTE_ARRAY : Bytes::ToBytes(value)));

}

Cell::Cell(): row_(""), family_(""), qualifier_(""), timestamp_(HBaseConstants::HConstants::LATEST_TIMESTAMP),
    value_(""), tags_(""), cellType_(CellType::UNKNOWN), key_value_(nullptr) {

}

Cell::Cell(const Cell &cell) {

  this->row_ = cell.row_;
  this->family_ = cell.family_;
  this->qualifier_ = cell.qualifier_;
  this->timestamp_ = cell.timestamp_;
  this->value_ = cell.value_;
  this->tags_ = cell.tags_;
  this->cellType_ = cell.cellType_;
  if (nullptr != cell.key_value_)
    this->key_value_  = new KeyValue(*cell.key_value_);
  else
    this->key_value_  = nullptr;
}

Cell& Cell::operator= (const Cell &cell) {

  this->row_ = cell.row_;
  this->family_ = cell.family_;
  this->qualifier_ = cell.qualifier_;
  this->timestamp_ = cell.timestamp_;
  this->value_ = cell.value_;
  this->tags_ = cell.tags_;
  this->cellType_ = cell.cellType_;
  if (nullptr!= cell.key_value_)
    this->key_value_ = new KeyValue(*cell.key_value_);
  else
    this->key_value_  = nullptr;
  return *this;

}
void Cell::Display() {

  LOG(INFO)		<< "Cell Contents: "
      << " Row: " << (row_.size() > 0 ? row_ : "")
      << "; Family: " << (family_.size() > 0 ? family_ : "")
      << "; Qualifer: " << (qualifier_.size() > 0 ? qualifier_ : "")
      << "; Tags: " << (tags_.size() > 0 ? tags_ : "")
      << "; Value: " << (value_.size() > 0 ? value_ : "")
      << "; Timestamp: " << timestamp_
      << "; CellType: " << static_cast<int>(cellType_);
}

const CellType::cellType &Cell::TypeByte() {
  return this->cellType_;
}

const std::string &Cell::Row() {
  return this->row_;
}

const std::string &Cell::Family() {
  return this->family_;
}

const std::string &Cell::Qualifier() {
  return this->qualifier_;
}

const std::string &Cell::Value() {
  return this->value_;
}

const long &Cell::Timestamp() {
  return this->timestamp_;
}


void Cell::DisplayKeyValueDetails() {

  BYTE_ARRAY tmp_rowbytes;
  Bytes::CopyByteArray( this->key_value_->GetRowArray(), tmp_rowbytes, this->key_value_->GetRowOffset(), this->key_value_->GetRowLength());

  BYTE_ARRAY tmp_fambytes;
  Bytes::CopyByteArray( this->key_value_->GetFamilyArray(), tmp_fambytes, this->key_value_->GetFamilyOffset(), this->key_value_->GetFamilyLength());


  BYTE_ARRAY tmp_qualbytes;
  Bytes::CopyByteArray( this->key_value_->GetQualifierArray(), tmp_qualbytes, this->key_value_->GetQualifierOffset(), this->key_value_->GetQualifierLength());


  BYTE_ARRAY tmp_valbytes;
  Bytes::CopyByteArray( this->key_value_->GetValueArray(), tmp_valbytes, this->key_value_->GetValueOffset(), this->key_value_->GetValueLength());


  Bytes::DisplayBytes(tmp_rowbytes);
  Bytes::DisplayBytes(tmp_fambytes);
  Bytes::DisplayBytes(tmp_qualbytes);
  DLOG(INFO)  << "Timestamp " << this->key_value_->GetTimestamp();
  DLOG(INFO)  << "Type " << static_cast<unsigned int>(this->key_value_->GetTypeByte());
  Bytes::DisplayBytes(tmp_valbytes);
}

Cell* Cell::CreateCell(const BYTE_ARRAY &bytes) {

  KeyValue *key_value = new KeyValue(bytes);

  BYTE_ARRAY tmp_rowbytes;
  if (key_value->GetRowLength())
    Bytes::CopyByteArray( key_value->GetRowArray(), tmp_rowbytes, key_value->GetRowOffset(), key_value->GetRowLength());

  BYTE_ARRAY tmp_fambytes;
  if(key_value->GetFamilyLength())
    Bytes::CopyByteArray( key_value->GetFamilyArray(), tmp_fambytes, key_value->GetFamilyOffset(), key_value->GetFamilyLength());

  BYTE_ARRAY tmp_qualbytes;
  if (key_value->GetQualifierLength())
    Bytes::CopyByteArray( key_value->GetQualifierArray(), tmp_qualbytes, key_value->GetQualifierOffset(), key_value->GetQualifierLength());

  BYTE_ARRAY tmp_valbytes;
  if (key_value->GetValueLength())
    Bytes::CopyByteArray( key_value->GetValueArray(), tmp_valbytes, key_value->GetValueOffset(), key_value->GetValueLength());

  long timestamp = key_value->GetTimestamp();
  unsigned int key_type = static_cast<unsigned int>(key_value->GetTypeByte());
  CellType::cellType cell_type = static_cast<CellType::cellType>(key_type);
  std::string tags("");
  delete key_value;

  Cell *cell = new Cell( Bytes::ToString(tmp_rowbytes), Bytes::ToString(tmp_fambytes), Bytes::ToString(tmp_qualbytes),
      timestamp, Bytes::ToString(tmp_valbytes), tags, cell_type);

  DLOG(INFO) << "row: " << Bytes::ToHex(tmp_rowbytes);
  DLOG(INFO) << "family: " << Bytes::ToHex(tmp_fambytes);
  DLOG(INFO) << "quaifier: " << Bytes::ToHex(tmp_qualbytes);
  DLOG(INFO) << "timestamp: " << timestamp;
  DLOG(INFO) << "value: " << Bytes::ToHex(tmp_valbytes);
  DLOG(INFO) << "cell_type: " << cell_type;

  return cell;
}

const KeyValue* Cell::GetKeyValue() const {

  return this->key_value_;
}

Cell* Cell::Parse(ByteBuffer &cell_data) {

  DLOG(INFO) << "cell_data.size() = " << cell_data.size();
  DLOG(INFO) << "cell_data: " << Bytes::ToHex(cell_data);
  int offset = 0;
  unsigned int cell_size_length;
  const char *pCurrent = &cell_data[offset];
  unsigned int *pSize = (unsigned int*) pCurrent;
  cell_size_length = *pSize;
  CommonUtils::SwapByteOrder(cell_size_length);
  pCurrent += Bytes::SIZEOF_INT;
  offset += Bytes::SIZEOF_INT;
  DLOG(INFO) << "cell_size: " << cell_size_length;

  BYTE_ARRAY bytes;
  bytes.insert(bytes.end(), pCurrent, pCurrent + cell_size_length);
  return Cell::CreateCell(bytes);
}

