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
#include "binary_comparator.h"

const char *BinaryComparator::COMPARATOR_NAME = "org.apache.hadoop.hbase.filter.BinaryComparator";

BinaryComparator::BinaryComparator(const BYTE_ARRAY &byte_array):ByteArrayComparable(byte_array) {
  // TODO Auto-generated constructor stub
}

BinaryComparator::~BinaryComparator() {
  // TODO Auto-generated destructor stub
}

int BinaryComparator::CompareTo(const BYTE_ARRAY &byte_array, const int &offset, const int &length) {
  return 0;
}

const char *BinaryComparator::GetName() {
  return BinaryComparator::COMPARATOR_NAME;
}

int BinaryComparator::ToByteArray(std::string &serialized_string) {

  hbase::pb::ByteArrayComparable byte_array_comp;
  google::protobuf::Message *msg = &byte_array_comp;
  std::unique_ptr<google::protobuf::Message> comparable_msg(this->Convert());
  msg = comparable_msg.get();
  if (nullptr != msg) {
    hbase::pb::ByteArrayComparable *byte_array_comp_resp = dynamic_cast<hbase::pb::ByteArrayComparable *>(msg);
    hbase::pb::BinaryComparator *comparator = new hbase::pb::BinaryComparator();
    comparator->set_allocated_comparable(byte_array_comp_resp);
    serialized_string = comparator->SerializeAsString();
  }

  return serialized_string.size();
}
