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

#include "operation_with_attributes.h"

#include <iostream>

const std::string OperationWithAttributes::ID_ATRIBUTE = "_operation.attributes.id";

OperationWithAttributes::OperationWithAttributes() {
  // TODO Auto-generated constructor stub

}

OperationWithAttributes::~OperationWithAttributes() {
  // TODO Auto-generated destructor stub
}


int OperationWithAttributes::GetAttribute(const std::string &name, BYTE_ARRAY &attr_value) {

  int attr_size = 0;
  if (attributes_.end() != attributes_.find(name)) {
    attr_size = attributes_[name].size();
  }
  return attr_size;
}

const OperationWithAttributes::ATTRIBUTE_MAP& OperationWithAttributes::GetAttributesMap() {

  return attributes_;
}

const long OperationWithAttributes::GetAttributeSize() {

  return attributes_.size(); // total of num map elements + num of keys + vector size in Java

}

OperationWithAttributes OperationWithAttributes::SetAttribute(const std::string &name, const BYTE_ARRAY &value) {

  if (0 == value.size()) {
    if(attributes_.size() > 0){
      attributes_[name].clear();
      attributes_.erase(name);
    }
  } else {
    BYTE_ARRAY attr_value;
    attr_value.insert (attr_value.end(), value.begin(), value.end());
    attributes_[name] = attr_value;
  }
  return *this;
}

OperationWithAttributes OperationWithAttributes::SetId(const std::string &id) {
  BYTE_ARRAY id_value;
  id_value.insert(id_value.end(), id.begin(), id.end());
  SetAttribute(OperationWithAttributes::ID_ATRIBUTE, id_value);
  return *this;
}

const int OperationWithAttributes::GetId(std::string &id_string) {
  BYTE_ARRAY id_value;
  int attr_size = GetAttribute(OperationWithAttributes::ID_ATRIBUTE, id_value);
  id_string = "";
  if (id_value.size() > 0) {
    id_string.insert(id_string.end(), id_value.begin(), id_value.end());
  }
  return attr_size;
}

