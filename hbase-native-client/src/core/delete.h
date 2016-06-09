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
#include "cell.h"
#include "key_value.h"
#include "mutation.h"


class Delete : public Mutation {

 public:
  Delete(const byte *row);
  Delete(const BYTE_ARRAY &row);
  Delete(const BYTE_ARRAY &row, const long &timestamp);
  Delete(const BYTE_ARRAY &row, const int &row_offset, const int &row_length);
  Delete(const BYTE_ARRAY &row, const int &row_offset, const int &row_length, const long &ts);
  Delete(const Delete &cDelete);
  Delete &operator=(const Delete &cDelete);

  virtual ~Delete();
  Delete& AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier);
  Delete& AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier, const long &timestamp);
  Delete& AddColumns(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier);
  Delete& AddColumns(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier, const long &timestamp);
  Delete& AddFamily(const BYTE_ARRAY &family);
  Delete& AddFamily(const BYTE_ARRAY &family, const long &timestamp);
  Delete& AddFamilyVersion(const BYTE_ARRAY &family, const long &timestamp);

};
