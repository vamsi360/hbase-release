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
/*
 * main application to perform the unit test
 */

#include "TestClient.h"

#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <unistd.h>
#include <Poco/Timestamp.h>
#include <glog/logging.h>
#include "../core/exception.h"
#include "../core/util.h"

int main(int argc, char *argv[]) {

	// Initialize Google's logging library.
	google::InitGoogleLogging(argv[0]);
	Poco::Timestamp start_ts;
	//TODO we can use this flags in test methods to execute a method or not
	bool break_loop = false;

	if (argc < 2) {
		std::string error_str = "Usage: ";
		error_str += argv[0];
		error_str += " ";
		error_str += "[Path to HBase Configuration File]\nEg:";
		error_str += argv[0];
		error_str += " ";
		error_str += "/etc/conf/hbase-site.xml";

		LOG(ERROR) << error_str;
		google::ShutdownGoogleLogging();
		exit (EXIT_FAILURE);
	}

	std::string user("anonymous");
	std::unique_ptr < Connection > conn(GetConnection(argv[1], user));

	if (nullptr == conn.get()) {
		throw HBaseException("Unable to get a connection to Zookeper Quorum");
	}

	Connection &connection = const_cast<Connection &>(*conn.get());
	std::string rowKey("row-4528");
	std::string rowKey2("row-5273");
	std::string delTableName("testns:testtable");

	int counter = 0;
	while (!break_loop) {
		try {
			delTableName += ".";
			delTableName += std::to_string(getpid());
			delTableName += std::to_string(time(0));
			//delTableName = "testns:testtable.157181461221337";
			//delTableName = "testtable-mr1";
			rowKey = "row-1";
			std::unique_ptr < Table
					> table(connection.GetTable(TableName::ValueOf(delTableName)));
			std::unique_ptr < Admin > admin(connection.GetAdmin());
			BYTE_ARRAY row;
			Bytes::ToBytes(rowKey, row);

			BYTE_ARRAY row2;
			Bytes::ToBytes(rowKey2, row2);

			std::vector < std::string > rows;
			rows.push_back("row-11255");
			rows.push_back("row-11256");
			rows.push_back("row-11257");
			rows.push_back("row-11258");
			//#if 0
			rows.push_back("row-23");
			rows.push_back("row-334");
			rows.push_back("row-445");
			rows.push_back("row-1352");
			rows.push_back("row-1753");
			//#endif
			std::vector < std::string > rows_deletes;
			rows_deletes.push_back("row-11255");
			rows_deletes.push_back("row-1352");
			rows_deletes.push_back("row-445");
			if (0) {
				//	TestPuts(connection, table, rows);
				//	TestGets(connection, table, rows);

				//	TestDeletes(connection, table, rows_deletes);
				//	TestGets(connection, table, rows);
			}

			if (1) {
				TestListTables(connection, admin, delTableName);
				usleep (SLEEP_TIME);

				TestCreateTable(connection, admin, delTableName);
				usleep(SLEEP_TIME);

				TestListTables(connection, admin, delTableName);
				usleep(SLEEP_TIME);

				TestPut(connection, table, row);
				usleep(SLEEP_TIME);
				TestPuts(connection, table, rows);
				usleep(SLEEP_TIME);

				TestScan (table);
				usleep(SLEEP_TIME);
				// Testcases for SCAN operation by mentioning columnfamily qualifier startrow and endrow
				BYTE_ARRAY family_;
				Bytes::ToBytes("colfam1", family_);
				BYTE_ARRAY qual_;
				Bytes::ToBytes("col1", qual_);

				std::string startrow("row-2");
				std::string stoprow("row-4");

				LOG(INFO) << "Test Scan Table  ";
				TestScan(table);
				LOG(INFO) << "Test Scan add family  ";
				TestScanAddfamily(table, family_);
				//LOG(INFO) << "Test Scan add column  ";
				//Test_Scan_addColumn(table,family_,qual_);
				LOG(INFO) << "Test Scan Start row  ";
				TestScanStartrow(table, startrow);
				LOG(INFO) << "Test Scan Stop row  ";
				TestScanStoprow(table, startrow, stoprow);
				LOG(INFO) << "Test Scan Reversed Table  ";
				TestScanReversed(table);

				TestGet(connection, table, row);
				usleep(SLEEP_TIME);
				TestGets(connection, table, rows);
				usleep(SLEEP_TIME);

				// Below version of Delete can be used to delete a specific family, qualifier, or all depending on the use case
				/*
				 * Test_Delete(connection, table, row, KeyValue::KEY_TYPE::Delete, 0);
				 * Test_Delete(connection, table, row, KeyValue::KEY_TYPE::Delete, 1461063451991L);
				 * Test_Delete(connection, table, row, KeyValue::KEY_TYPE::DeleteColumn, 0);
				 * Test_Delete(connection, table, row, KeyValue::KEY_TYPE::DeleteColumn, 1461070446759L);
				 * Test_Delete(connection, table, row, KeyValue::KEY_TYPE::DeleteFamily, 0);
				 * Test_Delete(connection, table, row, KeyValue::KEY_TYPE::DeleteFamily, 1461073313899L);
				 *
				 */
				TestDelete(connection, table, row, KeyValue::KEY_TYPE::Delete, 0);

				usleep(SLEEP_TIME);
				TestDeletes(connection, table, rows_deletes);
				usleep(SLEEP_TIME);

				TestDisableTable(connection, admin, delTableName);
				usleep(SLEEP_TIME);

				TestEnableTable(connection, admin, delTableName);
				usleep(SLEEP_TIME);

				TestDisableTable(connection, admin, delTableName);
				usleep(SLEEP_TIME);

				TestDeleteTable(connection, admin, delTableName);
				usleep(SLEEP_TIME);

				LOG(INFO) << "Table List after deleting " << delTableName << std::endl;
				TestListTables(connection, admin, "");
				usleep(SLEEP_TIME);
				break_loop = true;

			}
			table->close();
		} catch (const std::exception &exc) {
			// catch anything thrown within try block that derives from std::exception
			LOG(ERROR) << exc.what();
			break_loop = true;
		} catch (...) {
			LOG(ERROR) << "Unknown exception occured.";
			break_loop = true;
		}
		counter += 1;
		if (counter == MAX_COUNTER)
			break_loop = true;
		else {
			LOG(INFO)
					<< "=======================================================================";
			sleep(5);
		}
	}

	conn->Close();

	Poco::Timestamp end_ts;
	LOG(INFO) << " Total Time taken in secs:["
			<< (end_ts.raw() - start_ts.raw()) / 1000000 << "];";

	google::ShutdownGoogleLogging();
	google::protobuf::ShutdownProtobufLibrary();
}
