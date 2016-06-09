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

#include "mutation.h"

#include <sstream>
#include <glog/logging.h>

#include "exception.h"

/**
 * The attribute for storing the list of clusters that have consumed the change.
 */
const std::string Mutation::CONSUMED_CLUSTER_IDS = "_cs.id";

/**
 * The attribute for storing TTL for the result of the mutation.
 */
const std::string Mutation::OP_ATTRIBUTE_TTL = "_ttl";

Mutation::~Mutation() {

}

Mutation::Mutation()
    : ts_(HBaseConstants::HConstants::LATEST_TIMESTAMP),
      durability_(HBaseConstants::DURABILITY_TYPE::USE_DEFAULT) {

}

Mutation::Mutation(const Mutation &cmutation) {

  this->row_ = cmutation.row_;
  this->ts_ = cmutation.ts_;
  this->family_map_ = cmutation.family_map_;
}

Mutation& Mutation::operator=(const Mutation &cmutation) {

  this->row_ = cmutation.row_;
  this->ts_ = cmutation.ts_;
  this->family_map_ = cmutation.family_map_;

  return *this;

}
int Mutation::GetAcl(BYTE_ARRAY &attr_value) {
  return GetAttribute(HBaseConstants::AccessControlConstants::OP_ATTRIBUTE_ACL,
                      attr_value);
}

Mutation Mutation::SetAcl(const std::string &user, Permission perms) {
  BYTE_ARRAY attr_value;
  SetAttribute(HBaseConstants::AccessControlConstants::OP_ATTRIBUTE_ACL,
               attr_value);
  return *this;
}

Mutation Mutation::SetAcl(const std::map<std::string, Permission> &perms) {
  BYTE_ARRAY attr_value;
  std::map<std::string, Permission>::const_iterator itr;
  for (itr = perms.begin(); itr != perms.end(); ++itr) {

  }
  SetAttribute(HBaseConstants::AccessControlConstants::OP_ATTRIBUTE_ACL,
               attr_value);
  return *this;
}

const long Mutation::GetTtl(BYTE_ARRAY &attr_value) {

  long ttl = HBaseConstants::HConstants::LATEST_TIMESTAMP;
  int attr_size = GetAttribute(Mutation::OP_ATTRIBUTE_TTL, attr_value);
  if (attr_size > 0) {
    ttl = Bytes::ToLong(attr_value);
  }
  return ttl;
}

Mutation Mutation::SetTtl(const long &ttl) {

  BYTE_ARRAY attr_value;
  Bytes::ToBytes(ttl, attr_value);
  SetAttribute(Mutation::OP_ATTRIBUTE_TTL, attr_value);
  return *this;
}

ReturnCodes Mutation::CheckRow(const BYTE_ARRAY &row) {

  int row_length = row.size();
  if (0 == row_length) {
    throw HBaseException("Row length can't be 0");
  }
  if (row_length > HBaseConstants::HConstants::MAX_ROW_LENGTH) {
    std::stringstream str_error;
    str_error.str("");
    str_error << "Row length: " << row_length
        << " is greater than max row size: "
        << HBaseConstants::HConstants::MAX_ROW_LENGTH << std::endl;
    throw HBaseException(str_error.str());
  }
  return ReturnCodes::SUCCESS;
}

KeyValue *Mutation::CreatePutKeyValue(const BYTE_ARRAY &family,
                                      const BYTE_ARRAY &qualifier,
                                      const long &ts, const BYTE_ARRAY &value) {
  KeyValue *key_value = nullptr;
  key_value = new KeyValue(this->row_, family, qualifier, ts, value);
  return key_value;
}

const std::string Mutation::GetRowAsString() const {
  const std::string row_str(Bytes::ToString(this->row_));
  return row_str;
}

const FAMILY_MAP &Mutation::GetFamilyMap() const {

  return this->family_map_;
}

void Mutation::Display() {

  Bytes::DisplayBytes(this->row_);
  DLOG(INFO) << "Mutate TS: " << this->ts_;
  for (const auto &family : this->family_map_) {
    std::string qual_val_ts("");
    for (const auto &key_value : family.second) {
      BYTE_ARRAY qualifier;
      BYTE_ARRAY value;
      if (key_value.GetQualifierLength() > 0) {
        Bytes::CopyByteArray(key_value.GetQualifierArray(), qualifier,
                             key_value.GetQualifierOffset(),
                             key_value.GetQualifierLength());
      }
      if (key_value.GetValueLength() > 0) {
        Bytes::CopyByteArray(key_value.GetValueArray(), value,
                             key_value.GetValueOffset(),
                             key_value.GetValueLength());
      }
      const long &timestamp = key_value.GetTimestamp();
      qual_val_ts += (
          (key_value.GetQualifierLength() > 0) ?
              Bytes::ToString(qualifier) : "NO_QUALIFIER");
      qual_val_ts += "::";
      qual_val_ts += (
          (key_value.GetQualifierLength() > 0) ?
              Bytes::ToString(value) : "NO_VALUE");
      qual_val_ts += "::";
      qual_val_ts += std::to_string(timestamp);
      qual_val_ts += ";";
    }
    DLOG(INFO) << Bytes::ToString(family.first) << "[" << qual_val_ts << "]";
  }

}

