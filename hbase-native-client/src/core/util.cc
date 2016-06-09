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

#include "util.h"
#include <glog/logging.h>

void SwapByteOrder(unsigned int& ui) {
  ui = (ui >> 24) | ((ui << 8) & 0x00FF0000) | ((ui >> 8) & 0x0000FF00)
      | (ui << 24);
}

void SwapByteOrder2Bytes(unsigned short &us) {
  us = ((((us) >> 8) & 0x00FF) | (((us) << 8) & 0xFF00));
}

void Tokenize(const std::string &in, const std::string &delimiter,
              std::vector<std::string>&out) {

  std::string::size_type last_pos = in.find_first_not_of(delimiter, 0);
  std::string::size_type pos = in.find_first_of(delimiter, last_pos);

  while (std::string::npos != pos || std::string::npos != last_pos) {
    out.push_back(in.substr(last_pos, pos - last_pos));
    last_pos = in.find_first_not_of(delimiter, pos);
    pos = in.find_first_of(delimiter, last_pos);
  }
}

void GetTableAndNamespace(const std::string &tableName,
                          std::string &nameSpaceValue,
                          std::string &tableNameValue) {
  std::vector<std::string> table_details;
  Tokenize(tableName, ":", table_details);
  if (table_details.size() > 2) {
    DLOG(INFO) << "Incorrect Tablename specified";
  } else {
    if (2 == table_details.size()) {
      nameSpaceValue = table_details[0];
      tableNameValue = table_details[1];
    } else {
      nameSpaceValue = "default";
      tableNameValue = table_details[0];
    }
  }
  return;
}
