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


#include "query.h"

const std::string Query::ISOLATION_LEVEL = "_isolationlevel_";

Query::Query():
                target_replicaid_(-1),
                consistency_(CONSISTENCY::STRONG),
                filter_(nullptr) {

  // TODO Auto-generated constructor stub
}

Query::~Query() {

  // TODO Auto-generated destructor stub
}

int Query::GetAcl(BYTE_ARRAY &attr_value){

  return GetAttribute(HBaseConstants::AccessControlConstants::OP_ATTRIBUTE_ACL, attr_value);
}

//! TODO
Query &Query::SetAcl(const std::string &user, Permission perms) {

  BYTE_ARRAY attr_value;
  SetAttribute(HBaseConstants::AccessControlConstants::OP_ATTRIBUTE_ACL, attr_value);
  return *this;
}

//! TODO
Query &Query::SetAcl(const std::map<std::string, Permission> &perms) {

  BYTE_ARRAY attr_value;
  std::map<std::string, Permission>::const_iterator itr;
  for (itr = perms.begin(); itr != perms.end(); ++itr) {

  }
  SetAttribute(HBaseConstants::AccessControlConstants::OP_ATTRIBUTE_ACL, attr_value);
  return *this;
}

Filter *Query::GetFilter(){

  return this->filter_;
}

Query &Query::SetFilter(const Filter &filter){

  this->filter_ = const_cast<Filter *>(&filter);
  return *this;
}

/**
 * Specify region replica id where Query will fetch data from. Use this together with
 * {@link #setConsistency(Consistency)} passing {@link Consistency#TIMELINE} to read data from
 * a specific replicaId.
 * <br><b> Expert: </b>This is an advanced API exposed. Only use it if you know what you are doing
 * @param Id
 */
Query &Query::SetReplicaId(const int &replica_id) {
  this->target_replicaid_ = replica_id;
  return *this;
}

/**
 * Returns region replica id where Query will fetch data from.
 * @return region replica id or -1 if not set.
 */
const int &Query::GetReplicaId() const {
  return this->target_replicaid_;
}
