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

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <stddef.h>

#include <iostream>
#include <vector>

using byte = char;
using BYTE_TYPE = char;
using BYTE_ARRAY = std::vector<BYTE_TYPE>;
using ByteBuffer = std::vector<BYTE_TYPE>;

class Bytes {
 public:

  static const int SIZEOF_BOOLEAN;
  static const int SIZEOF_BYTE;
  static const int SIZEOF_CHAR;
  static const int SIZEOF_DOUBLE;
  static const int SIZEOF_FLOAT;
  static const int SIZEOF_INT;
  static const int SIZEOF_LONG;
  static const int SIZEOF_SHORT;
  static const char HEX_CHARS[];

  Bytes();
  Bytes(const Bytes &bytes);
  Bytes& operator= (const Bytes &bytes);

  Bytes(const BYTE_ARRAY &bytes);
  Bytes(const BYTE_ARRAY &bytes, const int &offset, const int &length);
  const BYTE_ARRAY &Get() const;
  void Set(const BYTE_ARRAY &bytes);
  void Set(const BYTE_ARRAY &bytes, const int &offset, const int &length);
  const int &GetOffset();
  const int &GetLength();

  virtual ~Bytes();

  static void DisplayBytes(const BYTE_ARRAY &bytes);
  static size_t StrLen(const char *);

  static BYTE_ARRAY ToBytes(const std::string &val, const bool &change_case_tolower = false);
  static BYTE_ARRAY ToBytes(const char *charstr_to_bytes);
  static BYTE_ARRAY ToBytes(const bool &val);


  static int ToBytes(const std::string &val, BYTE_ARRAY &bytes);
  static int ToBytes(const int &val, BYTE_ARRAY &bytes);
  static int ToBytes(const short &val, BYTE_ARRAY &bytes);
  static int ToBytes(const long &val, BYTE_ARRAY &bytes);

  static int ToString(const BYTE_ARRAY &byte, std::string &bytes_to_str);
  static std::string ToString(const BYTE_ARRAY &byte);
  static std::string ToString(const BYTE_ARRAY &bytes, const int &offset);
  static std::string ToString(const BYTE_ARRAY &byte, const int &offset, const int &length);
  static char ToByte(const BYTE_ARRAY &bytes);

  static int ToInt(const BYTE_ARRAY &bytes);
  static int ToInt(const BYTE_ARRAY &bytes, const int &offset);
  static int ToInt(const BYTE_ARRAY &bytes, const int &offset, const int &length);

  static short ToShort(const BYTE_ARRAY &bytes);
  static short ToShort(const BYTE_ARRAY &bytes, const int &offset);
  static short ToShort(const BYTE_ARRAY &bytes, const int &offset, const int &length);

  static long ToLong(const BYTE_ARRAY &bytes);
  static long ToLong(const BYTE_ARRAY &bytes, const int &offset);
  static long ToLong(const BYTE_ARRAY &bytes, const int &offset, const int &length);

  static unsigned long ToULong(const BYTE_ARRAY &bytes);
  static unsigned long ToULong(const BYTE_ARRAY &bytes, const int &offset);
  static unsigned long ToULong(const BYTE_ARRAY &bytes, const int &offset, const int &length);

  static float ToFloat(const BYTE_ARRAY &bytes);
  static double ToDouble(const BYTE_ARRAY &bytes);

  static std::string ToHex(const BYTE_ARRAY &bytes);
  static std::string ToHex(const BYTE_ARRAY &bytes, const int &offset, const int &length);

  static int PutInt(BYTE_ARRAY &bytes, const int &offset, const int &val);
  static int PutShort(BYTE_ARRAY &bytes, const int &offset, const short &val);
  static int PutLong(BYTE_ARRAY &bytes, const int &offset, const long &val);
  static int PutByte(BYTE_ARRAY &bytes, const int &offset, const BYTE_TYPE &val);
  static int PutBytes(BYTE_ARRAY &bytes, const int &from_index, const BYTE_ARRAY &bytes_to_put,
                      const int &offset, const int &num_bytes);


  static bool Equals(const BYTE_ARRAY &left, const BYTE_ARRAY &right);
  static int CopyByteArray( const BYTE_ARRAY &ip_bytes, BYTE_ARRAY &op_bytes,
                            const int &offset, const int &length);
 private:
  static bool ByteCompare(const BYTE_TYPE &left, const BYTE_TYPE &right);

  BYTE_ARRAY byte_array_;
  int offset_;
  int length_;
};
