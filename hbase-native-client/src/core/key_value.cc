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

#include "key_value.h"

#include <algorithm>
#include <iterator>
#include <glog/logging.h>

#include "hconstants.h"

const byte KeyValue::COLUMN_FAMILY_DELIMITER = ':';

const int KeyValue::KEY_LENGTH_SIZE = Bytes::SIZEOF_INT;


const int KeyValue::ROW_LENGTH_SIZE = Bytes::SIZEOF_SHORT;
const int KeyValue::FAMILY_LENGTH_SIZE = Bytes::SIZEOF_BYTE;
const int KeyValue::TIMESTAMP_SIZE = Bytes::SIZEOF_LONG;
const int KeyValue::TYPE_SIZE = Bytes::SIZEOF_BYTE;
const int KeyValue::TIMESTAMP_TYPE_SIZE = TIMESTAMP_SIZE + TYPE_SIZE;
const int KeyValue::KEY_INFRASTRUCTURE_SIZE = ROW_LENGTH_SIZE + FAMILY_LENGTH_SIZE + TIMESTAMP_TYPE_SIZE;

const int KeyValue::VALUE_LENGTH_SIZE = Bytes::SIZEOF_INT;
const int KeyValue::KEYVALUE_INFRASTRUCTURE_SIZE = KeyValue::KEY_LENGTH_SIZE /*keylength*/ + KeyValue::VALUE_LENGTH_SIZE /*valuelength*/;

const int KeyValue::ROW_OFFSET =  KeyValue::KEYVALUE_INFRASTRUCTURE_SIZE;
const int KeyValue::ROW_KEY_OFFSET = ROW_OFFSET + ROW_LENGTH_SIZE;

const int KeyValue::TAGS_LENGTH_SIZE = Bytes::SIZEOF_SHORT;
const int KeyValue::KEYVALUE_WITH_TAGS_INFRASTRUCTURE_SIZE = ROW_OFFSET + TAGS_LENGTH_SIZE;

KeyValue::KeyValue():offset_(-1), length_(-1), seqid_(0) {

}

KeyValue::KeyValue(const KeyValue& ckey_value) {

  //DLOG(INFO)  << "KV::Copy Constr";
  this->offset_ = ckey_value.offset_;
  this->length_ = ckey_value.length_;
  Bytes::CopyByteArray(ckey_value.bytes_, this->bytes_, this->offset_, this->length_);
  this->seqid_ = ckey_value.seqid_;
}

KeyValue& KeyValue::operator= (const KeyValue &ckey_value) {

  //DLOG(INFO)  << "KV::Assign Constr" ;
  this->offset_ = ckey_value.offset_;
  this->length_ = ckey_value.length_;
  Bytes::CopyByteArray(ckey_value.bytes_, this->bytes_, this->offset_, this->length_);
  this->seqid_ = ckey_value.seqid_;

  return *this;

}

KeyValue::KeyValue(const BYTE_ARRAY &bytes): KeyValue(bytes, 0) {

}

KeyValue::KeyValue(const BYTE_ARRAY &bytes, const int &offset):KeyValue(bytes, offset, GetLength(bytes, offset)) {

}

KeyValue::KeyValue( const BYTE_ARRAY &bytes, const int &offset, const int &length):offset_(offset), length_(length), seqid_(0) {

  Bytes::CopyByteArray(bytes, this->bytes_, this->offset_, this->length_);
}


KeyValue::KeyValue(const BYTE_ARRAY &bytes, const int &offset, const int &length, const long &ts):
                                  KeyValue(bytes, offset, length, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY, 0, 0, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY, 0, 0, ts,
                                      static_cast<BYTE_TYPE>(KeyValue::KEY_TYPE::Maximum), HBaseConstants::HConstants::EMPTY_BYTE_ARRAY, 0, 0, null_tags_) {

}


KeyValue::KeyValue(const BYTE_ARRAY &row, const long &timestamp):
                            KeyValue(row, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY, timestamp, static_cast<BYTE_TYPE>(KeyValue::KEY_TYPE::Maximum), HBaseConstants::HConstants::EMPTY_BYTE_ARRAY) {

}

KeyValue::KeyValue(const BYTE_ARRAY &row, const long &timestamp, const BYTE_TYPE &type):
                                  KeyValue(row, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY, timestamp, type, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY) {

}

KeyValue::KeyValue(const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier):
                            KeyValue(row, family, qualifier, HBaseConstants::HConstants::LATEST_TIMESTAMP, static_cast<BYTE_TYPE>(KeyValue::KEY_TYPE::Maximum)) {

}

KeyValue::KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
    const BYTE_ARRAY &value):
                                            KeyValue(row, family, qualifier,
                                                HBaseConstants::HConstants::LATEST_TIMESTAMP, static_cast<BYTE_TYPE>(KeyValue::KEY_TYPE::Put), value) {


}

KeyValue::KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
    const long &timestamp, const BYTE_TYPE &type):
                                           KeyValue(row, family, qualifier, timestamp, type, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY) {

}

KeyValue::KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
    const long &timestamp, const BYTE_ARRAY &value):KeyValue(row, family, qualifier,
        timestamp, static_cast<BYTE_TYPE>(KeyValue::KEY_TYPE::Put), value) {

}

KeyValue::KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
    const long &timestamp, const BYTE_ARRAY &value, const Tag *tag_ptr):
                                  KeyValue(row, family, qualifier, timestamp, value, null_tags_){

}

KeyValue::KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
    const long &timestamp, const BYTE_ARRAY &value, const std::vector<Tag> &tags):
                                            KeyValue( row, 0, row.size(), family, 0,  family.size(), qualifier, 0, qualifier.size(),
                                                timestamp, static_cast<BYTE_TYPE>(KeyValue::KEY_TYPE::Put), value, 0, value.size(), tags) {

}

KeyValue::KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
    const long &timestamp, const BYTE_TYPE &type, const BYTE_ARRAY &value):
                                    KeyValue(row, 0, row.size(), family, 0, family.size(), qualifier, 0, qualifier.size(), timestamp, type, value, 0, value.size()) {

}

KeyValue::KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
    const long &timestamp, const BYTE_TYPE &type, const BYTE_ARRAY &value,
    const std::vector<Tag> &tags):offset_(-1), length_(-1), seqid_(0) {

  KeyValue( row, family, qualifier, 0, qualifier.size(), timestamp, type, value, 0, value.size(), tags);
}

KeyValue::KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
    const int &qoffset, const int &qlength, const long &timestamp,
    const BYTE_TYPE &type, const BYTE_ARRAY &value, const int &voffset,
    const int &vlength, const std::vector<Tag> &tags):offset_(-1), length_(-1), seqid_(0)  {
  KeyValue(row, 0, row.size(), family, 0, family.size(), qualifier, qoffset, qlength, timestamp, type, value, voffset, vlength, tags);

}

KeyValue::KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
    const long &timestamp, const BYTE_TYPE &type, const BYTE_ARRAY &value,
    const BYTE_ARRAY &tag_bytes):
                                  KeyValue(row, family, qualifier, 0, qualifier.size(), timestamp, type, value, 0, value.size(), null_tags_) {

}

KeyValue::KeyValue( BYTE_ARRAY &buffer, const int &boffset,
    const BYTE_ARRAY &row, const int &roffset, const int &rlength,
    const BYTE_ARRAY &family, const int &foffset, const int &flength,
    const BYTE_ARRAY &qualifier, const int &qoffset, const int &qlength,
    const long &timestamp, const BYTE_TYPE &type,
    const BYTE_ARRAY &value, const int &voffset, const int &vlength,
    const BYTE_ARRAY &tag_bytes):offset_(-1), length_(-1), seqid_(0) {

}

KeyValue::KeyValue( const BYTE_ARRAY &row, const int &row_offset, const int &row_length,
    const BYTE_ARRAY &family, const int &family_offset, const int &family_length,
    const BYTE_ARRAY &qualifier, const int &qualifier_offset, const int &qualifier_length,
    const long &timestamp, const BYTE_TYPE &type,
    const BYTE_ARRAY &value, const int &value_offset, const int &value_length,
    const BYTE_ARRAY &tag_bytes, const int &tagsOffset, const int &tagsLength):offset_(-1), length_(-1), seqid_(0){


  this->length_ = CreateByteArray( this->bytes_, row, row_offset, row_length,
      family, family_offset, family_length,
      qualifier, qualifier_offset, qualifier_length,
      timestamp, type, value, value_offset, value_length, tag_bytes, tagsOffset, tagsLength);
  this->offset_ = 0;
}

KeyValue::KeyValue( const int &rlength, const int &flength, const int &qlength, const long &timestamp, const BYTE_TYPE &type,
    const int &vlength):offset_(-1), length_(-1), seqid_(0) {
  KeyValue(rlength, flength, qlength, timestamp, type, vlength, 0);
}

KeyValue::KeyValue( const int &rlength, const int &flength, const int &qlength,
    const long &timestamp, const BYTE_TYPE &type, const int &vlength,
    const int &tags_length):offset_(-1), length_(-1), seqid_(0) {

}

KeyValue::KeyValue( const BYTE_ARRAY &row, const int &roffset, const int &rlength,
    const BYTE_ARRAY &family, const int &foffset, const int &flength,
    const BYTE_ARRAY &qualifier, const int &qoffset, const int &qlength,
    const long &timestamp, const BYTE_TYPE &type,
    const BYTE_ARRAY &value, const int &voffset, const int &vlength):
                                            KeyValue( row, roffset, rlength, family, foffset, flength, qualifier,
                                                qoffset, qlength, timestamp, type, value, voffset, vlength, null_tags_){

}

KeyValue::KeyValue( const BYTE_ARRAY &row, const int &row_offset, const int &row_length,
    const BYTE_ARRAY &family, const int &family_offset, const int &family_length,
    const BYTE_ARRAY &qualifier, const int &qualifier_offset, const int &qualifier_length,
    const long &timestamp, const BYTE_TYPE &type,
    const BYTE_ARRAY &value, const int &value_offset, const int &value_length,
    const std::vector<Tag> &tags):offset_(-1), length_(-1), seqid_(0){

  this->length_ = CreateByteArray( this->bytes_, row, row_offset, row_length,
      family, family_offset, family_length,
      qualifier, qualifier_offset, qualifier_length,
      timestamp, type, value, value_offset, value_length, tags);
  this->offset_ = 0;

}

int KeyValue::WriteByteArray(BYTE_ARRAY &key_value, const int &boffset,
    const BYTE_ARRAY &row, const int &roffset, const int &rlength,
    const BYTE_ARRAY &family, const int &foffset, const int &flength,
    const BYTE_ARRAY &qualifier, const int &qoffset, const int &qlength,
    const long timestamp, const BYTE_TYPE &type,
    const BYTE_ARRAY &value, const int &voffset, const int &vlength, const BYTE_ARRAY &tags) {

  int key_length = static_cast<int>(GetKeyDataStructureSize(rlength, flength, qlength));
  int key_value_length = static_cast<int>(GetKeyValueDataStructureSize( rlength, flength, qlength, vlength, tags.size()));
  key_value.resize(key_value_length);
  // Write key, value and key row length.
  int pos = boffset;
  pos = Bytes::PutInt(key_value, pos, key_length);
  pos = Bytes::PutInt(key_value, pos, vlength);
  pos = Bytes::PutShort(key_value, pos, (short)(rlength & 0x0000ffff));
  pos = Bytes::PutBytes(key_value, pos, row, roffset, rlength);
  pos = Bytes::PutByte(key_value, pos, (byte) (flength & 0x0000ff));
  if (flength != 0) {
    pos = Bytes::PutBytes(key_value, pos, family, foffset, flength);
  }
  if (qlength != 0) {
    pos = Bytes::PutBytes(key_value, pos, qualifier, qoffset, qlength);
  }
  pos = Bytes::PutLong(key_value, pos, timestamp);
  pos = Bytes::PutByte(key_value, pos, type);
  if (value.size()> 0) {
    pos = Bytes::PutBytes(key_value, pos, value, voffset, vlength);
  }
  return key_value_length;
}

int KeyValue::CreateByteArray( BYTE_ARRAY &key_value,
    const BYTE_ARRAY &row, const int &row_offset, const int &row_length,
    const BYTE_ARRAY &family, const int &family_offset, const int &family_length,
    const BYTE_ARRAY &qualifier, const int &qualifier_offset, const int &qualifier_length,
    const long &timestamp, const BYTE_TYPE &type,
    const BYTE_ARRAY &value, const int &value_offset, const int &value_length,
    const std::vector<Tag> &tags){

  int key_length = static_cast<int>(GetKeyDataStructureSize(row_length, family_length, qualifier_length));
  int key_value_length = static_cast<int>(GetKeyValueDataStructureSize( row_length, family_length, qualifier_length,
      value_length, tags.size()));
  key_value.resize(key_value_length);

  int pos = 0;
  pos = Bytes::PutInt(key_value, pos, key_length);
  pos = Bytes::PutInt(key_value, pos, value_length);
  pos = Bytes::PutShort(key_value, pos, static_cast<short>(row_length & 0x0000ffff));
  pos = Bytes::PutBytes(key_value, pos, row, row_offset, row_length);
  pos = Bytes::PutByte(key_value, pos, static_cast<byte>(family_length & 0x0000FF));
  if(family_length != 0) {
    pos = Bytes::PutBytes(key_value, pos, family, family_offset, family_length);
  }
  if (qualifier_length > 0) {
    pos = Bytes::PutBytes(key_value, pos, qualifier, qualifier_offset, qualifier_length);
  }
  pos = Bytes::PutLong(key_value, pos, timestamp);
  pos = Bytes::PutByte(key_value, pos, type);
  if (value_length > 0) {
    pos = Bytes::PutBytes(key_value, pos, value, value_offset, value_length);
  }
  return key_value.size();

}

int KeyValue::CreateByteArray( BYTE_ARRAY &key_value,
    const BYTE_ARRAY &row, const int &row_offset, const int &row_length,
    const BYTE_ARRAY &family, const int &family_offset, const int &family_length,
    const BYTE_ARRAY &qualifier, const int &qualifier_offset, const int &qualifier_length,
    const long &timestamp, const BYTE_TYPE &type,
    const BYTE_ARRAY &value, const int &value_offset, const int &value_length,
    const BYTE_ARRAY &tag_bytes, const int &tag_offset, const int &tag_length){

  int key_length = static_cast<int>(GetKeyDataStructureSize(row_length, family_length, qualifier_length));
  int key_value_length = static_cast<int>(GetKeyValueDataStructureSize( row_length, family_length, qualifier_length,
      value_length, tag_length));
  key_value.resize(key_value_length);

  int pos = 0;
  pos = Bytes::PutInt(key_value, pos, key_length);
  pos = Bytes::PutInt(key_value, pos, value_length);
  pos = Bytes::PutShort(key_value, pos, static_cast<short>(row_length & 0x0000ffff));
  pos = Bytes::PutBytes(key_value, pos, row, row_offset, row_length);
  pos = Bytes::PutByte(key_value, pos, static_cast<byte>(family_length & 0x0000FF));
  if(family_length != 0) {
    pos = Bytes::PutBytes(key_value, pos, family, family_offset, family_length);
  }
  if (qualifier_length > 0) {
    pos = Bytes::PutBytes(key_value, pos, qualifier, qualifier_offset, qualifier_length);
  }
  pos = Bytes::PutLong(key_value, pos, timestamp);
  pos = Bytes::PutByte(key_value, pos, type);
  if (value_length > 0) {
    pos = Bytes::PutBytes(key_value, pos, value, value_offset, value_length);
  }

  return key_value.size();
}

KeyValue::~KeyValue() {
}


long KeyValue::GetKeyValueDataStructureSize(  const int &rlength,
    const int &flength,
    const int &qlength,
    const int &vlength,
    const int &tagsLength){

  long kv_ds_size = 0L;
  kv_ds_size = GetKeyDataStructureSize(rlength, flength, qlength) + vlength;
  if (0 == tagsLength) {
    kv_ds_size += KeyValue::KEYVALUE_INFRASTRUCTURE_SIZE;
  }else{
    kv_ds_size +=  KeyValue::KEYVALUE_WITH_TAGS_INFRASTRUCTURE_SIZE + tagsLength;
  }
  return kv_ds_size;
}


long KeyValue::GetKeyDataStructureSize(  const int &rlength,
    const int &flength,
    const int &qlength) {
  return KeyValue::KEY_INFRASTRUCTURE_SIZE + rlength + flength + qlength;
}

void KeyValue::SetSequenceId(const long &sequence_id){

  this->seqid_ = sequence_id;
}


void KeyValue::DisplayBytes(){

  Bytes::DisplayBytes(this->bytes_);
  DLOG(INFO)  << "Hex Vals:: " << Bytes::ToHex(this->bytes_);

}

int KeyValue::GetLength(const BYTE_ARRAY &bytes, const int &offset){

  int klength = KeyValue::ROW_OFFSET + Bytes::ToInt(bytes, offset);
  int vlength = Bytes::ToInt(bytes, offset + Bytes::SIZEOF_INT);
  return klength + vlength;
}

const int KeyValue::GetKeyOffset() const{
  return this->offset_ + KeyValue::ROW_OFFSET;
}

const int KeyValue::GetKeyLength() const{

  return Bytes::ToInt(this->bytes_, this->offset_);
}


const BYTE_ARRAY &KeyValue::GetRowArray() const{
  return this->bytes_;
}

const int KeyValue::GetRowOffset() const{

  return this->offset_ + KeyValue::ROW_KEY_OFFSET;
}

const int KeyValue::GetRowLength() const{

  return Bytes::ToShort(this->bytes_, GetKeyOffset());
}

const BYTE_ARRAY &KeyValue::GetFamilyArray() const{
  return this->bytes_;
}

const int KeyValue::GetFamilyOffset() const{

  return GetFamilyOffset(GetRowLength());
}

const int KeyValue::GetFamilyOffset(const int &row_length) const{

  return this->offset_ + KeyValue::ROW_KEY_OFFSET + row_length + KeyValue::FAMILY_LENGTH_SIZE;
}

const BYTE_TYPE KeyValue::GetFamilyLength() const{

  return GetFamilyLength(GetFamilyOffset());
}

const BYTE_TYPE KeyValue::GetFamilyLength(const int &foffset) const{

  return this->bytes_[foffset-1];
}

const BYTE_ARRAY &KeyValue::GetQualifierArray() const{

  return this->bytes_;
}

const int KeyValue::GetQualifierOffset() const{

  return GetQualifierOffset(GetFamilyOffset());
}

const int KeyValue::GetQualifierOffset(const int &foffset) const{

  return foffset + GetFamilyLength(foffset);
}

const int KeyValue::GetQualifierLength() const{

  return GetQualifierLength(GetRowLength(),GetFamilyLength());

}

const int KeyValue::GetQualifierLength(const int &rlength, const int &flength) const{

  return GetKeyLength() - static_cast<int>(GetKeyDataStructureSize(rlength, flength, 0));
}

const int KeyValue::GetTimestampOffset() {

  return GetTimestampOffset(GetKeyLength());
}

const int KeyValue::GetTimestampOffset(const int &key_length) const{
  return GetKeyOffset() + key_length - KeyValue::TIMESTAMP_TYPE_SIZE;
}

const unsigned long KeyValue::GetTimestamp() const{

  return GetTimestamp(GetKeyLength());
}

const unsigned long KeyValue::GetTimestamp(const int &key_length) const{

  int ts_offset = GetTimestampOffset(key_length);
  return Bytes::ToLong(this->bytes_, ts_offset);
}

const BYTE_TYPE KeyValue::GetTypeByte() const{

  return this->bytes_[this->offset_ + GetKeyLength() - 1 + KeyValue::ROW_OFFSET];
}

const long KeyValue::GetSequenceId(){

  return this->seqid_;
}

const BYTE_ARRAY &KeyValue::GetValueArray() const{

  return this->bytes_;
}

const int KeyValue::GetValueOffset() const{

  int voffset = GetKeyOffset() + GetKeyLength();
  return voffset;
}

const int KeyValue::GetValueLength() const{

  int vlength = Bytes::ToInt(this->bytes_, this->offset_ + KeyValue::KEY_LENGTH_SIZE);
  return vlength;
}


const int &KeyValue::GetKeyValueLength() const{

  return this->length_;
}
