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

#include "substring_comparator.h"

#include <algorithm>

const char *SubstringComparator::COMPARATOR_NAME = "org.apache.hadoop.hbase.filter.SubstringComparator";

SubstringComparator::SubstringComparator(std::string substr):  ByteArrayComparable(Bytes::ToBytes(substr, true)),
    substr_(substr) {
  std::transform(substr_.begin(), substr_.end(), substr_.begin(), ::tolower);

}

SubstringComparator::~SubstringComparator() {
  // TODO Auto-generated destructor stub
}

int SubstringComparator::CompareTo(const BYTE_ARRAY &byte_array, const int &offset, const int &length) {

  std::string byte_string("");
  std::size_t pos = std::string::npos;
  BYTE_ARRAY byte_value;
  if (this->GetValue(byte_value) > 0) {
    byte_string = Bytes::ToString(byte_value);
    pos = byte_string.find(this->substr_);
  }

  return ((std::string::npos != pos) ? 0 : 1);

}

const char *SubstringComparator::GetName() {

  return SubstringComparator::COMPARATOR_NAME;
}

int SubstringComparator::ToByteArray(std::string &serialized_string) {

  hbase::pb::ByteArrayComparable byte_array_comp;
  google::protobuf::Message *msg = &byte_array_comp;
  std::unique_ptr<google::protobuf::Message> comparable_msg(this->Convert());
  msg = comparable_msg.get();
  if (nullptr != msg) {
    hbase::pb::ByteArrayComparable *byte_array_comp_resp = dynamic_cast<hbase::pb::ByteArrayComparable *>(msg);
    hbase::pb::SubstringComparator *comparator = new hbase::pb::SubstringComparator();
    comparator->set_substr(this->substr_);
    serialized_string = comparator->SerializeAsString();
  }

  return serialized_string.size();
}

