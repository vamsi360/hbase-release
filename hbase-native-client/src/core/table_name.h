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

class TableName {

 public:
  TableName();
  static TableName* ValueOf(const char *table_name);
  static TableName* ValueOf(const std::string &);
  static TableName* CreateTableNameIfNecessary(const std::string &nameSpace,
      const std::string &tableName);
  static void IsLegalFulyQualifiedTableName(const std::string &fullyQfdTableName);
  static void IsLegalNameSpaceName(const std::string &nameSpace);
  static void IsLegalTableQualifierName(const std::string &qualifier);
  TableName(const TableName &ctableName);
  TableName& operator= (const TableName &ctableName);
  virtual ~TableName();
  const std::string &GetTableNameAsString();
  const std::string &GetQualifierNameAsString();
  const std::string &GetName() const;
  bool operator <(const TableName &rhs) const;
  const static std::string NAMESPACE_DELIM;
  const static std::string DEFAULT_NAMESPACE;
  const static std::string SYSTEM_NAMESPACE;
  const static std::string OLD_META_STR;
  const static std::string OLD_ROOT_STR;

 private:
  std::string nameSpace_;
  std::string qualifier_;
  bool systemTable_;
  std::string name_; //nameSpace+:+qualifier

  TableName(const std::string &nameSpace, const std::string &tableName);
};

