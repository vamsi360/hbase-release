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

#include "connection.h"
#include "rpc_client.h"
#include "table_schema.h"

const int sleep_betwee_statuschk = 400000;

class Connection;

namespace TableState{
enum CurrentTableState{
  ENABLED = 0,
  DISABLED,
  DISABLING,
  ENABLING
};

enum CheckForTableState{
  ISENABLED = 0,
  ISDISABLED,
  ISEXISTS
};

enum TableOperationStatus {
  NOT_FOUND = 0,
  RUNNING,
  FINISHED
};
}

class Admin {
 public:
  Admin ();
  Admin (Connection *conn);
  virtual ~Admin();

  void CreateTable(TableSchema &table_schema);
  void EnableTable(const std::string &table_name);
  void DisableTable(const std::string &table_name);
  void DeleteTable(const std::string &table_name);
  bool TableExists(const std::string &table_name);
  bool IsEnabledTable(const std::string &table_name);
  bool IsDisabledTable(const std::string &table_name);
  void CheckTableState(const std::string &table_name);
  TableState::TableOperationStatus GetProcedureResponse(const int &procedure_id);
  void ListTables(std::vector<TableSchema> &table_list, const std::string &reg_ex = "", const bool &include_sys_tables = false);
  std::vector<TableSchema> &ListTables(const std::string &reg_ex = "", const bool &include_sys_tables = false);


 private:
  std::shared_ptr<RpcClient> rpc_client_;
  Connection *connection_;
};
