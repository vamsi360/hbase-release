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

#include "bytes.h"
#include "tag.h"

class KeyValue {
 public:
  enum class KEY_TYPE{
    Minimum = static_cast<BYTE_TYPE>(0),
    Put = static_cast<BYTE_TYPE>(4),
    Delete = static_cast<BYTE_TYPE>(8),
    DeleteFamilyVersion = static_cast<BYTE_TYPE>(10),
    DeleteColumn = static_cast<BYTE_TYPE>(12),
    DeleteFamily = static_cast<BYTE_TYPE>(14),
    Maximum = static_cast<BYTE_TYPE>(255)
  };
  const static byte COLUMN_FAMILY_DELIMITER;
  /** Size of the key length field in bytes*/
  const static int KEY_LENGTH_SIZE;
  /** Size of the row length field in bytes */
  const static int ROW_LENGTH_SIZE;
  /** Size of the family length field in bytes */
  const static int FAMILY_LENGTH_SIZE;
  /** Size of the timestamp field in bytes */
  const static int TIMESTAMP_SIZE;
  /** Size of the key type field in bytes */
  const static int TYPE_SIZE;
  // Size of the timestamp and type byte on end of a key -- a long + a byte.
  const static int TIMESTAMP_TYPE_SIZE;
  // Size of the length shorts and bytes in key.
  const static int KEY_INFRASTRUCTURE_SIZE;
  // How far into the key the row starts at. First thing to read is the short
  // that says how long the row is.
  const static int ROW_OFFSET;
  const static int ROW_KEY_OFFSET;
  // Size of the length ints in a KeyValue datastructure.
  const static int KEYVALUE_INFRASTRUCTURE_SIZE;
  /** Size of the tags length field in bytes */
  const static int TAGS_LENGTH_SIZE;
  const static int KEYVALUE_WITH_TAGS_INFRASTRUCTURE_SIZE;

  const static int VALUE_LENGTH_SIZE;

  KeyValue();
  KeyValue(const KeyValue& ckey_value);
  KeyValue& operator= (const KeyValue &ckey_value);
  KeyValue( const BYTE_ARRAY &bytes);
  KeyValue( const BYTE_ARRAY &bytes, const int &offset);
  KeyValue( const BYTE_ARRAY &bytes, const int &offset, const int &length);
  KeyValue( const BYTE_ARRAY &bytes, const int &offset, const int &length, const long &ts);
  KeyValue( const BYTE_ARRAY &row, const long &timestamp);
  KeyValue(const BYTE_ARRAY &row, const long &timestamp, const BYTE_TYPE &type);
  KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier);
  KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
            const BYTE_ARRAY &value);
  KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
            const long &timestamp, const BYTE_TYPE &type);
  KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
            const long &timestamp, const BYTE_ARRAY &value);
  KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
            const long &timestamp, const BYTE_ARRAY &value, const Tag *tag_ptr);
  KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
            const long &timestamp, const BYTE_ARRAY &value, const std::vector<Tag> &tags);
  KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
            const long &timestamp, const BYTE_TYPE &type, const BYTE_ARRAY &value);
  KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
            const long &timestamp, const BYTE_TYPE &type, const BYTE_ARRAY &value,
            const std::vector<Tag> &tags);
  KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
            const int &qoffset, const int &qlength, const long &timestamp,
            const BYTE_TYPE &type, const BYTE_ARRAY &value, const int &voffset,
            const int & vlength, const std::vector<Tag> &tags);
  KeyValue( const BYTE_ARRAY &row, const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
            const long &timestamp, const BYTE_TYPE &type, const BYTE_ARRAY &value,
            const BYTE_ARRAY &tag_bytes);
  KeyValue( BYTE_ARRAY &buffer, const int &boffset,
            const BYTE_ARRAY &row, const int &roffset, const int &rlength,
            const BYTE_ARRAY &family, const int &foffset, const int &flength,
            const BYTE_ARRAY &qualifier, const int &qoffset, const int &qlength,
            const long &timestamp, const BYTE_TYPE &type,
            const BYTE_ARRAY &value, const int &voffset, const int &vlength,
            const BYTE_ARRAY &tag_bytes);
  KeyValue( const BYTE_ARRAY &row, const int &row_offset, const int &row_length,
            const BYTE_ARRAY &family, const int &family_offset, const int &family_length,
            const BYTE_ARRAY &qualifier, const int &qualifier_offset, const int &qualifier_length,
            const long &timestamp, const BYTE_TYPE &type,
            const BYTE_ARRAY &value, const int &value_offset, const int &value_length,
            const BYTE_ARRAY &tag_bytes, const int &tagsOffset, const int &tagsLength);
  KeyValue( const int &rlength, const int &flength, const int &qlength,
            const long &timestamp, const BYTE_TYPE &type, const int &vlength);
  KeyValue( const int &rlength, const int &flength, const int &qlength,
            const long &timestamp, const BYTE_TYPE &type, const int &vlength,
            const int &tags_length);
  KeyValue( const BYTE_ARRAY &row, const int &roffset, const int &rlength,
            const BYTE_ARRAY &family, const int &foffset, const int &flength,
            const BYTE_ARRAY &qualifier, const int &qoffset, const int &qlength,
            const long &timestamp, const BYTE_TYPE &type,
            const BYTE_ARRAY &value, const int &voffset, const int &vlength);
  KeyValue( const BYTE_ARRAY &row, const int &row_offset, const int &row_length,
            const BYTE_ARRAY &family, const int &family_offset, const int &family_length,
            const BYTE_ARRAY &qualifier, const int &qualifier_offset, const int &qualifier_length,
            const long &timestamp, const BYTE_TYPE &type,
            const BYTE_ARRAY &value, const int &value_offset, const int &value_length,
            const std::vector<Tag> &tags);
  const int GetKeyOffset() const;
  const int GetKeyLength() const;
  const BYTE_ARRAY &GetRowArray() const;
  const int GetRowOffset() const;
  const int GetRowLength() const;
  const BYTE_ARRAY &GetFamilyArray() const;
  const int GetFamilyOffset() const;
  const BYTE_TYPE GetFamilyLength() const;
  const BYTE_ARRAY &GetQualifierArray() const;
  const int GetQualifierOffset() const;
  const int GetQualifierLength() const;
  const int GetTimestampOffset();
  const unsigned long GetTimestamp() const;
  const BYTE_TYPE GetTypeByte() const;
  const long GetSequenceId();
  const BYTE_ARRAY &GetValueArray() const;
  const int GetValueOffset() const;
  const int GetValueLength() const;
  virtual ~KeyValue();
  void SetSequenceId(const long &sequence_id);
  void DisplayBytes();
  static long GetKeyValueDataStructureSize( const int &rlength, const int &flength,
                                            const int &qlength, const int &vlength, const int &tagsLength = 0);
  static long GetKeyDataStructureSize(  const int &rlength, const int &flength, const int &qlength);
  static int CreateByteArray( BYTE_ARRAY &key_value,
                              const BYTE_ARRAY &row, const int &row_offset, const int &row_length,
                              const BYTE_ARRAY &family, const int &family_offset, const int &family_length,
                              const BYTE_ARRAY &qualifier, const int &qualifier_offset, const int &qualifier_length,
                              const long &timestamp, const BYTE_TYPE &type,
                              const BYTE_ARRAY &value, const int &value_offset, const int &value_length,
                              const std::vector<Tag> &tags);
  static int CreateByteArray( BYTE_ARRAY &key_value,
                              const BYTE_ARRAY &row, const int &row_offset, const int &row_length,
                              const BYTE_ARRAY &family, const int &family_offset, const int &family_length,
                              const BYTE_ARRAY &qualifier, const int &qualifier_offset, const int &qualifier_length,
                              const long &timestamp, const BYTE_TYPE &type,
                              const BYTE_ARRAY &value, const int &value_offset, const int &value_length,
                              const BYTE_ARRAY &tag_bytes, const int &tag_offset, const int &tag_length);
  static int WriteByteArray(  BYTE_ARRAY &buffer, const int &boffset,
                              const BYTE_ARRAY &row, const int &roffset, const int &rlength,
                              const BYTE_ARRAY &family, const int &foffset, const int &flength,
                              const BYTE_ARRAY &qualifier, const int &qoffset, const int &qlength,
                              const long timestamp, const BYTE_TYPE & type,
                              const BYTE_ARRAY &value, const int &voffset, const int &vlength, const BYTE_ARRAY &tag_bytes);

  const int &GetKeyValueLength() const;

 protected:
  int offset_;           // offset into bytes buffer KV starts at
  int length_;           // length of the KV starting from offset.
  BYTE_ARRAY bytes_;     // an immutable byte array that contains the KV
  std::vector<Tag> null_tags_;

 private:
  const static int MAX_TAGS_LENGTH;
  long seqid_;
  const int GetFamilyOffset(const int &row_length) const;
  const BYTE_TYPE GetFamilyLength(const int &foffset) const;
  const int GetQualifierOffset(const int &foffset) const;
  const int GetQualifierLength(const int &rlength, const int &flength) const;
  const int GetTimestampOffset(const int &key_length) const;
  const unsigned long GetTimestamp(const int &key_length) const;
  static int GetLength(const BYTE_ARRAY &bytes, const int &offset);
};

