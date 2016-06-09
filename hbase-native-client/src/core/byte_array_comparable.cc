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

#include "byte_array_comparable.h"

ByteArrayComparable::ByteArrayComparable() {

}

ByteArrayComparable::ByteArrayComparable(const BYTE_ARRAY &value) {

  this->value_.insert(this->value_.end(), value.begin(), value.end());
}

ByteArrayComparable::~ByteArrayComparable() {
  // TODO Auto-generated destructor stub
}

ByteArrayComparable *ByteArrayComparable::ParseFrom(const BYTE_ARRAY &pb_bytes) {
  throw std::string("ParseFrom called on base ByteArrayComparable, but should be called on derived type");
}

int ByteArrayComparable::GetValue(BYTE_ARRAY &value) {

  value.insert(value.end(), this->value_.begin(), this->value_.end());
  return this->value_.size();
}

bool ByteArrayComparable::AreSerializedFieldsEqual(ByteArrayComparable &other) {

  BYTE_ARRAY to_compare;
  other.GetValue(to_compare);
  return Bytes::Equals(this->value_, to_compare);
}

int ByteArrayComparable::CompareTo(const BYTE_ARRAY & value) {

  return 0;
}

int ByteArrayComparable::CompareTo(const BYTE_ARRAY &value, const int &offset, const int &length) {

  return 0;
}

std::unique_ptr<google::protobuf::Message>ByteArrayComparable::Convert() {

  hbase::pb::ByteArrayComparable *comparable = new hbase::pb::ByteArrayComparable();
  if (this->value_.size() > 0) {
    comparable->set_value(Bytes::ToString(this->value_));
  }
  google::protobuf::Message *message = dynamic_cast<google::protobuf::Message *>(comparable);
  std::unique_ptr<google::protobuf::Message> ptr(message);

  return ptr;
}

