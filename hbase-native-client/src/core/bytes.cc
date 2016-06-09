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
#include "bytes.h"

#include <algorithm>
#include <sstream>
#include <glog/logging.h>

#include "exception.h"

/**
 * Size of boolean in bytes
 */
const int Bytes::SIZEOF_BOOLEAN = sizeof(BYTE_TYPE) / sizeof(BYTE_TYPE);
/**
 * Size of byte in bytes
 */
const int Bytes::SIZEOF_BYTE = Bytes::SIZEOF_BOOLEAN;
/**
 * Size of char in bytes
 */
const int Bytes::SIZEOF_CHAR = sizeof(short) / sizeof(BYTE_TYPE);
/**
 * Size of double in bytes
 */
const int Bytes::SIZEOF_DOUBLE = sizeof(double) / sizeof(BYTE_TYPE);
/**
 * Size of float in bytes
 */
const int Bytes::SIZEOF_FLOAT = sizeof(float) / sizeof(BYTE_TYPE);
/**
 * Size of int in bytes
 */
const int Bytes::SIZEOF_INT = sizeof(int) / sizeof(BYTE_TYPE);
/**
 * Size of long in bytes
 */
const int Bytes::SIZEOF_LONG = sizeof(long) / sizeof(BYTE_TYPE);
/**
 * Size of short in bytes
 */
const int Bytes::SIZEOF_SHORT = sizeof(short) / sizeof(BYTE_TYPE);

const char Bytes::HEX_CHARS[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8',
    '9', 'a', 'b', 'c', 'd', 'e', 'f', '\0' };

Bytes::Bytes()
    : offset_(-1),
      length_(0) {

  this->byte_array_ = {};
}

Bytes::Bytes(const BYTE_ARRAY &bytes)
    : Bytes(bytes, 0, bytes.size()) {

}

Bytes::Bytes(const BYTE_ARRAY &bytes, const int &offset, const int &length) {
  this->byte_array_ = bytes;
  this->offset_ = offset;
  this->length_ = length;
}

Bytes::Bytes(const Bytes &cbytes) {
  this->byte_array_ = cbytes.byte_array_;
  this->offset_ = cbytes.offset_;
  this->length_ = cbytes.length_;
}

Bytes& Bytes::operator=(const Bytes &cbytes) {
  this->byte_array_ = cbytes.byte_array_;
  this->offset_ = cbytes.offset_;
  this->length_ = cbytes.length_;
  return *this;
}

const BYTE_ARRAY &Bytes::Get() const {
  return this->byte_array_;
}

void Bytes::Set(const BYTE_ARRAY &bytes) {

  this->Set(bytes, 0, bytes.size());
}

void Bytes::Set(const BYTE_ARRAY &bytes, const int &offset, const int &length) {

  this->byte_array_ = bytes;
  this->offset_ = offset;
  this->length_ = length;
}

const int &Bytes::GetOffset() {

  return this->offset_;
}

const int &Bytes::GetLength() {

  return this->length_;
}

Bytes::~Bytes() {
  // TODO Auto-generated destructor stub
}

bool Bytes::ByteCompare(const BYTE_TYPE &left, const BYTE_TYPE &right) {

  return (left == right);
}

void Bytes::DisplayBytes(const BYTE_ARRAY &bytes) {

  std::stringstream bytes_str;
  bytes_str.str("");
  bytes_str << "Byte Array[";
  for (uint i = 0; i < bytes.size(); ++i) {
    bytes_str << bytes[i];
    if (i != bytes.size() - 1)
      bytes_str << " ";
  }
  bytes_str << "]";
  DLOG(INFO)<< bytes_str.str();
  bytes_str.str("");
}

size_t Bytes::StrLen(const char *s) {
  if (!s || NULL == s)
    return 0;
  else
    return ::strlen(s);
}

int Bytes::ToBytes(const std::string &str_to_bytes, BYTE_ARRAY &bytes) {

  if (str_to_bytes.size() > 0)
    bytes.insert(bytes.end(), str_to_bytes.begin(), str_to_bytes.end());

  return bytes.size();
}

int Bytes::ToBytes(const int &val, BYTE_ARRAY &bytes) {

  for (int i = 0, j = Bytes::SIZEOF_INT - 1; i < Bytes::SIZEOF_INT; i++, j--) {
    bytes.push_back(static_cast<BYTE_TYPE>((val >> (j * 8)) & 0xFF));
  }
  return bytes.size();
}

int Bytes::ToBytes(const short &val, BYTE_ARRAY &bytes) {

  for (int i = 0, j = Bytes::SIZEOF_SHORT - 1; i < Bytes::SIZEOF_SHORT;
      i++, j--) {
    bytes.push_back(static_cast<BYTE_TYPE>((val >> (j * 8)) & 0xFF));
  }

  return bytes.size();
}

int Bytes::ToBytes(const long &val, BYTE_ARRAY &bytes) {
  for (int i = 0, j = Bytes::SIZEOF_LONG - 1; i < Bytes::SIZEOF_LONG;
      i++, j--) {
    bytes.push_back(static_cast<BYTE_TYPE>((val >> (j * 8)) & 0xFF));
  }
  return bytes.size();
}

int Bytes::ToString(const BYTE_ARRAY &byte, std::string &bytes_to_str) {

  if (0 == byte.size())
    bytes_to_str = "";
  else
    bytes_to_str.insert(bytes_to_str.end(), byte.begin(), byte.end());
  return bytes_to_str.size();

}

std::string Bytes::ToString(const BYTE_ARRAY &byte) {

  return Bytes::ToString(byte, 0, byte.size());
}

std::string Bytes::ToString(const BYTE_ARRAY &byte, const int &offset) {

  return Bytes::ToString(byte, offset, byte.size());
}

std::string Bytes::ToString(const BYTE_ARRAY &byte, const int &offset,
                            const int &length) {

  std::string bytes_to_str("");
  if (0 == byte.size())
    bytes_to_str = "";
  else
    bytes_to_str.insert(bytes_to_str.end(), byte.begin(), byte.end());
  return bytes_to_str;
}

bool Bytes::Equals(const BYTE_ARRAY &left, const BYTE_ARRAY &right) {

  if (left.size() != right.size())
    return false;
  if (std::equal(left.begin(), left.end(), right.begin(), Bytes::ByteCompare))
    return true;
  else
    return false;

}

int Bytes::CopyByteArray(const BYTE_ARRAY &ip_bytes, BYTE_ARRAY &op_bytes,
                         const int &offset, const int &length) {
  int ip_size = ip_bytes.size();

  if (0 == ip_size)
    throw HBaseException("Can't copy empty byte array");

  if (offset < 0)
    throw HBaseException("Offset must be >= 0");

  if (0 == length) {
    throw HBaseException("Cant copy 0 bytes");
  }

  int max_bytes_can_copy = ip_size - offset;

  if (length > max_bytes_can_copy) {
    std::stringstream str_error;
    str_error.str("");
    str_error << "Cant copy " << length << " bytes as only "
              << max_bytes_can_copy << " can be copied." << std::endl;
    throw HBaseException(str_error.str());
  }

  op_bytes.resize(length);
  BYTE_ARRAY::const_iterator start_itr = ip_bytes.begin() + offset;
  BYTE_ARRAY::const_iterator end_itr = start_itr + length;
  std::copy(start_itr, end_itr, op_bytes.begin());

  return op_bytes.size();
}

int Bytes::PutInt(BYTE_ARRAY &bytes, const int &offset, const int &val) {

  if ((bytes.size() - offset) < Bytes::SIZEOF_INT) {
    std::stringstream str_error;
    str_error.str("");
    str_error << "Not enough room to put an int of size " << Bytes::SIZEOF_INT
        << " at offset " << offset << " in a " << bytes.size()
        << " byte array.";
    throw HBaseException(str_error.str());
  }

  for (int i = 0, j = Bytes::SIZEOF_INT - 1; i < Bytes::SIZEOF_INT; i++, j--) {
    bytes[offset + i] = static_cast<BYTE_TYPE>((val >> (j * 8)) & 0xFF);
  }

  return offset + Bytes::SIZEOF_INT;
}

int Bytes::PutShort(BYTE_ARRAY &bytes, const int &offset, const short &val) {

  if ((bytes.size() - offset) < Bytes::SIZEOF_SHORT) {
    std::stringstream str_error;
    str_error.str("");
    str_error << "Not enough room to put a short of size "
        << Bytes::SIZEOF_SHORT << " at offset " << offset << " in a "
        << bytes.size() << " byte array.";
    throw HBaseException(str_error.str());
  }

  for (int i = 0, j = Bytes::SIZEOF_SHORT - 1; i < Bytes::SIZEOF_SHORT;
      i++, j--) {
    bytes[offset + i] = static_cast<BYTE_TYPE>((val >> (j * 8)) & 0xFF);
  }

  return offset + Bytes::SIZEOF_SHORT;
}

int Bytes::PutLong(BYTE_ARRAY &bytes, const int &offset, const long &val) {

  if ((bytes.size() - offset) < Bytes::SIZEOF_LONG) {
    std::stringstream str_error;
    str_error.str("");
    str_error << "Not enough room to put a long of size " << Bytes::SIZEOF_LONG
        << " at offset " << offset << " in a " << bytes.size()
        << " byte array.";
    throw HBaseException(str_error.str());
  }

  for (int i = 0, j = Bytes::SIZEOF_LONG - 1; i < Bytes::SIZEOF_LONG;
      i++, j--) {
    bytes[offset + i] = static_cast<BYTE_TYPE>((val >> (j * 8)) & 0xFF);
  }

  return offset + SIZEOF_LONG;
}

int Bytes::PutByte(BYTE_ARRAY &bytes, const int &offset, const BYTE_TYPE &val) {

  if ((bytes.size() - offset) < Bytes::SIZEOF_BYTE) {
    std::stringstream str_error;
    str_error.str("");
    str_error << "Not enough room to put a byte of size " << Bytes::SIZEOF_BYTE
        << " at offset " << offset << " in a " << bytes.size()
        << " byte array.";
    throw HBaseException(str_error.str());
  }

  bytes[offset] = static_cast<BYTE_TYPE>((val));
  return offset + Bytes::SIZEOF_BYTE;
}

int Bytes::PutBytes(BYTE_ARRAY &dst_bytes, const int &dst_offset,
                    const BYTE_ARRAY &src_bytes, const int &src_offset,
                    const int &src_num_bytes) {

  if (src_num_bytes <= 0) {
    std::stringstream str_error;
    str_error.str("");
    str_error << "Cant add " << src_num_bytes << std::endl;
    throw HBaseException(str_error.str());
  }
  if (src_num_bytes > static_cast<int>(src_bytes.size())) {
    std::stringstream str_error;
    str_error.str("");
    str_error << "Cant add " << src_num_bytes
        << " bytes from source byte array size of " << src_bytes.size()
        << " bytes.";
    throw HBaseException(str_error.str());
  }
  if (static_cast<int>(dst_bytes.size()) < src_num_bytes) {

    std::stringstream str_error;
    str_error.str("");
    str_error << "Cant add " << src_num_bytes
        << " bytes to destinaion byte array size of " << dst_bytes.size()
        << " bytes.";
    throw HBaseException(str_error.str());
  }

  BYTE_ARRAY::const_iterator start_itr = src_bytes.begin() + src_offset;
  BYTE_ARRAY::const_iterator end_itr = start_itr + src_num_bytes;
  BYTE_ARRAY::iterator start_dest_itr = dst_bytes.begin() + dst_offset;
  std::copy(start_itr, end_itr, start_dest_itr);

  return dst_offset + src_num_bytes;
}

int Bytes::ToInt(const BYTE_ARRAY &bytes) {
  return Bytes::ToInt(bytes, 0, SIZEOF_INT);
}

int Bytes::ToInt(const BYTE_ARRAY &bytes, const int &offset) {
  return Bytes::ToInt(bytes, offset, SIZEOF_INT);

}

int Bytes::ToInt(const BYTE_ARRAY &bytes, const int &offset,
                 const int &length) {
  int int_val = 0;
  if (length != SIZEOF_INT
      || static_cast<unsigned int>(offset + length) > bytes.size()) {
    throw "WrongLengthOrOffset";  //explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
  } else {
    for (int i = offset, j = Bytes::SIZEOF_INT - 1; i < (offset + length);
        i++, j--) {
      int_val |= (bytes[i] & 0XFF) << (j * 8);
    }
  }
  return int_val;
}

short Bytes::ToShort(const BYTE_ARRAY &bytes) {
  return Bytes::ToShort(bytes, 0, SIZEOF_SHORT);
}

short Bytes::ToShort(const BYTE_ARRAY &bytes, const int &offset) {
  return Bytes::ToShort(bytes, offset, SIZEOF_SHORT);

}

short Bytes::ToShort(const BYTE_ARRAY &bytes, const int &offset,
                     const int &length) {
  short short_val = 0;
  if (length != SIZEOF_SHORT
      || static_cast<unsigned int>(offset + length) > bytes.size()) {
    throw "WrongLengthOrOffset";  //explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
  } else {
    for (int i = offset, j = Bytes::SIZEOF_SHORT - 1; i < (offset + length);
        i++, j--) {
      short_val |= (bytes[i] & 0XFF) << (j * 8);
    }
  }
  return short_val;
}

long Bytes::ToLong(const BYTE_ARRAY &bytes) {
  return Bytes::ToLong(bytes, 0, SIZEOF_LONG);
}

long Bytes::ToLong(const BYTE_ARRAY &bytes, const int &offset) {
  return Bytes::ToLong(bytes, offset, SIZEOF_LONG);

}

long Bytes::ToLong(const BYTE_ARRAY &bytes, const int &offset,
                   const int &length) {

  long long_val = 0L;
  if (length != SIZEOF_LONG
      || static_cast<unsigned int>(offset + length) > bytes.size()) {
    throw "WrongLengthOrOffset";  //explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
  } else {
    for (int i = offset, j = Bytes::SIZEOF_LONG - 1; i < (offset + length);
        i++, j--) {
      long_val <<= 8;
      long_val ^= bytes[i] & 0xFF;
    }
  }
  /*const char *pCurrent = &bytes[offset];
   PocoXXXX::UInt64 *pts = (PocoXXXX::UInt64*) pCurrent;
   long_val = *pts;
   long_val = PocoXXXX::ByteOrder::flipBytes(long_val);
   */
  return long_val;
}

unsigned long Bytes::ToULong(const BYTE_ARRAY &bytes) {
  return Bytes::ToULong(bytes, 0, SIZEOF_LONG);
}

unsigned long Bytes::ToULong(const BYTE_ARRAY &bytes, const int &offset) {
  return Bytes::ToULong(bytes, offset, SIZEOF_LONG);

}

unsigned long Bytes::ToULong(const BYTE_ARRAY &bytes, const int &offset,
                             const int &length) {

  unsigned long long_val = 0L;
  //const char *pCurrent = &bytes[offset];
  if (length != SIZEOF_LONG
      || static_cast<unsigned int>(offset + length) > bytes.size()) {
    throw "WrongLengthOrOffset";  //explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
  } else {
    for (int i = offset, j = Bytes::SIZEOF_LONG - 1; i < (offset + length);
        i++, j--) {
      long_val <<= 8;
      long_val ^= bytes[i] & 0xFF;
    }
  }
  /*
   DLOG(INFO)<< "SIZEOF_LONG is " << SIZEOF_LONG;

   DLOG(INFO)<< "long_val_tmp is " << long_val_tmp;

   PocoXXXX::UInt64 *pts = (PocoXXXX::UInt64*) pCurrent;
   long_val = *pts;
   long_val = PocoXXXX::ByteOrder::flipBytes(long_val);
   DLOG(INFO)<< "long_val is " << long_val;
   */
  return long_val;
}

std::string Bytes::ToHex(const BYTE_ARRAY &bytes) {
  return Bytes::ToHex(bytes, 0, bytes.size());
}

std::string Bytes::ToHex(const BYTE_ARRAY &bytes, const int &offset,
                         const int &length) {

  if (length > (std::numeric_limits<int>::max() / 2)) {

    throw HBaseException(
        "Illegal Argument Exception while converting byte array to hex, Check the length passed");
  }

  int num_chars = length * 2;
  std::string hex_str;
  hex_str.resize(num_chars);
  for (int i = 0; i < num_chars; i += 2) {
    BYTE_TYPE d = bytes[offset + i / 2];
    hex_str[i] = HEX_CHARS[(d >> 4) & 0x0F];
    hex_str[i + 1] = HEX_CHARS[d & 0x0F];
  }
  return hex_str;
}

BYTE_ARRAY Bytes::ToBytes(const char *charstr_to_bytes) {

  std::string str_to_bytes("");
  if (nullptr != charstr_to_bytes)
    str_to_bytes = charstr_to_bytes;
  return Bytes::ToBytes(str_to_bytes);
}

BYTE_ARRAY Bytes::ToBytes(const std::string &str_to_bytes, const bool &change_case_tolower) {

  BYTE_ARRAY bytes;
  if (str_to_bytes.size() > 0){
    if(change_case_tolower){
      std::string lower_str = str_to_bytes;
      std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(), ::tolower);
      bytes.insert(bytes.end(), lower_str.begin(), lower_str.end());
    }else{
      bytes.insert(bytes.end(), str_to_bytes.begin(), str_to_bytes.end());
    }
  }

  return bytes;
}

BYTE_ARRAY Bytes::ToBytes(const bool &val) {

  BYTE_ARRAY bytes;
  for (int i = 0, j = Bytes::SIZEOF_BOOLEAN; i < Bytes::SIZEOF_BOOLEAN;
      i++, j--) {
    bytes.push_back(static_cast<BYTE_TYPE>((val >> (j * 8)) & 0xFF));
  }

  return bytes;
}
