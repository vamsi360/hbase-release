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

#include <iostream>
#include <string>

#include "bytes.h"
#include "hconstants.h"
#include "key_value.h"
#include "operation_with_attributes.h"
#include "permission.h"
#include "return_codes.h"

using FAMILY_MAP = std::map<BYTE_ARRAY, std::vector<KeyValue>>;

enum class DELETE_TYPE {
  DELETE_ONE_VERSION,
  DELETE_MULTIPLE_VERSIONS,
  DELETE_FAMILY,
  DELETE_FAMILY_VERSION
};

class Mutation : public OperationWithAttributes {

 public:

  Mutation();
  Mutation(const Mutation &cmutation);
  Mutation& operator=(const Mutation &cmutation);
  virtual ~Mutation();
  int GetAcl(BYTE_ARRAY &attr_value);
  Mutation SetAcl(const std::string &user, Permission perms);
  Mutation SetAcl(const std::map<std::string, Permission> &perms);
  const long GetTtl(BYTE_ARRAY &attr_value);
  Mutation SetTtl(const long &ttl);
  KeyValue *CreatePutKeyValue(const BYTE_ARRAY &family,
                              const BYTE_ARRAY &qualifier, const long &ts,
                              const BYTE_ARRAY &value);
  const std::string GetRowAsString() const;
  const FAMILY_MAP &GetFamilyMap() const;
  void Display();
  static ReturnCodes CheckRow(const BYTE_ARRAY &row);

 protected:

  BYTE_ARRAY row_;
  long ts_;
  HBaseConstants::DURABILITY_TYPE durability_;
  FAMILY_MAP family_map_;

 private:

  /**
   * The attribute for storing the list of clusters that have consumed the change.
   */
  static const std::string CONSUMED_CLUSTER_IDS;

  /**
   * The attribute for storing TTL for the result of the mutation.
   */
  static const std::string OP_ATTRIBUTE_TTL;
};
