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

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "bytes.h"
#include "cell.h"
#include "consistency.h"
#include "hconstants.h"
#include "mutation.h"
#include "query.h"
#include "return_codes.h"

class Put : public Mutation {
 public:
  virtual ~Put();
  Put(const BYTE_ARRAY &row);
  Put(const BYTE_ARRAY &row, const long &timestamp);
  Put(const BYTE_ARRAY &row, const int &row_offset, const int &row_length);
  Put(const BYTE_ARRAY &row, const int &row_offset, const int &row_length,
      const long &ts);
  Put(const Put &cPut);
  Put& operator=(const Put &cPut);

  Put& Add(const KeyValue &key_value);
  Put& AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
              const BYTE_ARRAY &value);
  Put& AddColumn(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
                const long &ts, const BYTE_ARRAY &value);

  int GetAcl(BYTE_ARRAY &attr_value);
  Put& SetAcl(const std::string &user, Permission perms);
  Put& SetAcl(const std::map<std::string, Permission> &perms);

  const long GetTtl(BYTE_ARRAY &attr_value);
  Put& SetTtl(const long &ttl);

 int Get(const BYTE_ARRAY &family, const BYTE_ARRAY &qualifier,
       std::vector<Cell> &cellList);
};
