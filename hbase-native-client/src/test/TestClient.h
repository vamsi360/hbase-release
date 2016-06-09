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
