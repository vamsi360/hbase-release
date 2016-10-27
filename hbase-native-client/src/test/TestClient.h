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

#include "../../src/core/connection.h"

const int SLEEP_TIME = 0;
const int MAX_COUNTER = 1;
Connection* GetConnection(const char *xmlPath, std::string & user);
void TestMetaCache(Connection *conn);
void TestPut(Connection &connection, std::unique_ptr<Table> &table,
						 const BYTE_ARRAY &rowKey);
void TestPuts(Connection &connection, std::unique_ptr<Table> &table,
							std::vector<std::string> &rowKeys);
void TestGet(Connection &connection, std::unique_ptr<Table> &table,
						 const BYTE_ARRAY &rowKey);
void TestGets(Connection &connection, std::unique_ptr<Table> &table,
							std::vector<std::string>&rowKeys);
void TestDelete(Connection &connection, std::unique_ptr<Table> &table,
								const BYTE_ARRAY &rowKey, const KeyValue::KEY_TYPE &delete_type,
								const long &ts);
void TestDeletes(Connection &connection, std::unique_ptr<Table> &table,
								 std::vector<std::string> &rowKeys);
void TestIsEnabledTable(Connection &connection, std::unique_ptr<Admin> &admin,
												const std::string &tableName);
void TestEnableTable(Connection &connection, std::unique_ptr<Admin> &admin,
										 const std::string &tableName);
void TestDisableTable(Connection &connection, std::unique_ptr<Admin> &admin,
											const std::string &tableName);
void TestCreateTable(Connection &connection, std::unique_ptr<Admin> &admin,
										 const std::string &tableName);
void TestDeleteTable(Connection &connection, std::unique_ptr<Admin> &admin,
										 const std::string &tableName);
void TestListTables(Connection &connection, std::unique_ptr<Admin> &admin,
										const std::string &tableName);
void ReleaseConnection(Connection* conn);
void TestScan(std::unique_ptr<Table> &table);
void TestScanReversed(std::unique_ptr<Table> &table);
void TestScanStoprow(std::unique_ptr<Table> &table, std::string start_row,
										 std::string end_row);
void TestScanStartrow(std::unique_ptr<Table> &table, std::string start_row);
void TestScanAddColumn(std::unique_ptr<Table> &table, BYTE_ARRAY family_,
											 BYTE_ARRAY qual_);
void TestScanAddfamily(std::unique_ptr<Table> &table, BYTE_ARRAY family_);
void ScanResult(ResultScanner *scanner, std::unique_ptr<Table> &table);
int CreateKeyValue(BYTE_ARRAY &bytes);
void ParseDetailsFromKeyValue(BYTE_ARRAY &bytes);
