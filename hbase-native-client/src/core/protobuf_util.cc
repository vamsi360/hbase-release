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

#include "protobuf_util.h"
#include "connection.h"

void SetRequestHeader(hbase::pb::RequestHeader &rh, const std::string &methodName, const bool &requestParam) {
  rh.set_call_id(Connection::SetRpcCallId());
  std::string *method_name = new std::string(methodName);
  rh.set_allocated_method_name(method_name);
  rh.set_request_param(requestParam);
  return;
}
