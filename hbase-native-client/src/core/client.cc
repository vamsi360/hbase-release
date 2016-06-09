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

#include "client.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <cstdio>
#include <Poco/ByteOrder.h>
#include <glog/logging.h>

#include "hconstants.h"
#include "family_filter.h"
#include "qualifier_filter.h"
#include "value_filter.h"
#include "row_filter.h"
#include "binary_comparator.h"
#include "substring_comparator.h"
#include "configuration.h"
#include "connection.h"
#include "connection_factory.h"
#include "key_value.h"
#include "table_name.h"
#include "bytes.h"
#include "get.h"
#include "put.h"
#include "delete.h"
#include "table.h"
#include "cell.h"
#include "result.h"
#include "table_schema.h"
#include "column_family_schema.h"
#include "admin.h"
#include "tag.h"
#include "utils.h"

const std::string GET_COLFAMILY("colfam1");
const std::string GET_COLQUALIFIER("col-1");

const std::string GET_COLFAMILY3("colfam3");
const std::string SOMEOTHERFAMILY("someotherfamily");

Connection* GetConnection(const char *xmlPath, std::string &user) {

  Connection *connection = nullptr;
  Configuration *hBaseConfig = new Configuration(xmlPath);
  connection = ConnectionFactory::CreateConnection(hBaseConfig, user);
  const std::string &zkQuorum = hBaseConfig->GetValue("hbase.zookeeper.quorum");
  const std::string &client_retries = hBaseConfig->GetValue(
      "hbase.client.retries.number");
  DLOG(INFO)<< "[" << __func__ << "]" << "[" << __func__ << "]" << "Quorum is "
  << zkQuorum;
  connection->SetZkQuorum(zkQuorum.c_str());
  connection->Init();
  connection->SetClientRetries(atoi(client_retries.c_str()));

  return connection;
}

void Test_Find_Master_Server(Connection &connection) {
  std::string master_server_ip;
  int master_server_port;
  connection.FindMasterServer(master_server_ip, master_server_port);

  DLOG(INFO)<< "Master Server IP  : " << master_server_ip;
  DLOG(INFO)<< "Master Server Port: " << master_server_port;

}

void TestPut(Connection &connection, std::unique_ptr<Table> &table,
             const BYTE_ARRAY &rowKey) {

  Put put(rowKey);
  put.Display();

  BYTE_ARRAY family;
  BYTE_ARRAY family3;
  BYTE_ARRAY qualifier;
  BYTE_ARRAY value;
  BYTE_ARRAY someotherfamily;

  Bytes::ToBytes(GET_COLFAMILY, family);
  Bytes::ToBytes(GET_COLFAMILY3, family3);
  Bytes::ToBytes(SOMEOTHERFAMILY, someotherfamily);

  Bytes::ToBytes(GET_COLQUALIFIER, qualifier);
  Bytes::ToBytes("Test From C++ client with CF:col-1", value);
  put.AddColumn(family, qualifier, value);
  value.clear();
  Bytes::ToBytes("Test From C++ client NO CF", value);
  put.AddColumn(family, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY, value);
  put.Display();

  qualifier.clear();
  value.clear();
  Bytes::ToBytes("qual1", qualifier);
  Bytes::ToBytes("Test From C++ client:CF:qual1", value);

  qualifier.clear();
  value.clear();
  Bytes::ToBytes("qual2", qualifier);
  Bytes::ToBytes("Test From C++ client:CF:qual2", value);

  qualifier.clear();
  value.clear();
  Bytes::ToBytes("qual3", qualifier);
  Bytes::ToBytes("Test From C++ client:CF:qual3", value);

  value.clear();
  Bytes::ToBytes("Only value for testing", value);
  const std::string val("Only value for testing");
  if (table->put(put)) {
    LOG(INFO)<< "[" << __func__ << "]" << "[" << __func__ << "]"
    << "Put Successful for : " << put.GetRowAsString() << " in "
    << table->getName()->GetName();
  } else {
    LOG(INFO) << "[" << __func__ << "]" << "[" << __func__ << "]"
    << "Put Unsuccessful for : " << put.GetRowAsString() << " in "
    << table->getName()->GetName();
  }
}

void TestPuts(Connection &connection, std::unique_ptr<Table> &table,
              std::vector<std::string> &rowKeys) {

  std::vector<Put *> puts;
  for (auto row : rowKeys) {
    DLOG(INFO)<< "Row is " << row;
    BYTE_ARRAY rowKey = Bytes::ToBytes(row);
    Put *put = new Put(rowKey);
    BYTE_ARRAY family;
    BYTE_ARRAY family3;
    BYTE_ARRAY qualifier;
    BYTE_ARRAY value;
    BYTE_ARRAY someotherfamily;

    Bytes::ToBytes(GET_COLFAMILY, family);
    Bytes::ToBytes(GET_COLFAMILY3, family3);
    Bytes::ToBytes(SOMEOTHERFAMILY, someotherfamily);

    Bytes::ToBytes(GET_COLQUALIFIER, qualifier);
    Bytes::ToBytes("Test From C++ client:CF:col-1", value);
    put->AddColumn(family, qualifier, value);

    qualifier.clear();
    value.clear();
    Bytes::ToBytes("qual1", qualifier);
    Bytes::ToBytes("Test From C++ client:CF:qual1", value);
    put->AddColumn(family, qualifier, value);

    qualifier.clear();
    value.clear();
    Bytes::ToBytes("qual2", qualifier);
    Bytes::ToBytes("Test From C++ client:CF:qual2", value);
    put->AddColumn(family, qualifier, 1455059962217, value);

    qualifier.clear();
    value.clear();
    Bytes::ToBytes("qual3", qualifier);
    Bytes::ToBytes("Test From C++ client:CF:qual3", value);
    put->AddColumn(family, qualifier, 1454850000000, value);

    value.clear();
    Bytes::ToBytes("Only value for testing", value);
    const std::string val("Only value for testing");
    //put->AddColumn(family3, HBaseConstants::HConstants::EMPTY_BYTE_ARRAY, Bytes::ToBytes(val));
    puts.push_back(put);
  }

  if (table->put(const_cast<std::vector<Put *>&>(puts))) {

    LOG(INFO)<< "[" << __func__ << "]" << "Multi-Put Successful" << std::endl;
  } else {
    LOG(INFO) << "[" << __func__ << "]" << "Multi-Delete Unsuccessful"
    << std::endl;
  }

  for (auto &op : puts) {
    if (nullptr != op) {
      delete op;
      op = nullptr;
    }
  }
  puts.clear();
}

FamilyFilter *GetFamilyFilter(const int &filter_num,
                              const BYTE_ARRAY &get_family,
                              const std::string &string) {

  FamilyFilter *filter_ptr = nullptr;
  BinaryComparator *bin_comparator = new BinaryComparator(get_family);
  SubstringComparator *substr_comparator = new SubstringComparator(string);

  switch (filter_num) {
    case 1:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::EQUAL,
                                    bin_comparator);
      break;
    case 2:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::GREATER,
                                    bin_comparator);
      break;
    case 3:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::GREATER_OR_EQUAL,
                                    bin_comparator);
      break;
    case 4:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::LESS,
                                    bin_comparator);
      break;
    case 5:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::LESS_OR_EQUAL,
                                    bin_comparator);
      break;
    case 6:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::NOT_EQUAL,
                                    bin_comparator);
      break;
    case 7:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::NO_OP,
                                    bin_comparator);
      break;
    case 8:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::EQUAL,
                                    substr_comparator);
      break;
    case 9:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::GREATER,
                                    substr_comparator);
      break;
    case 10:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::GREATER_OR_EQUAL,
                                    substr_comparator);
      break;
    case 11:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::LESS,
                                    substr_comparator);
      break;
    case 12:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::LESS_OR_EQUAL,
                                    substr_comparator);
      break;
    case 13:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::NOT_EQUAL,
                                    substr_comparator);
      break;
    case 14:
      filter_ptr = new FamilyFilter(CompareFilter::CompareOp::NO_OP,
                                    substr_comparator);
      break;
  }
  return filter_ptr;
}

QualifierFilter *GetQualifierFilter(const int &filter_num,
                                    const BYTE_ARRAY &get_qualifier,
                                    const std::string &string) {

  QualifierFilter *filter_ptr = nullptr;
  BinaryComparator *bin_comparator = new BinaryComparator(get_qualifier);
  SubstringComparator *substr_comparator = new SubstringComparator(string);

  switch (filter_num) {
    case 1:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::EQUAL,
                                       bin_comparator);
      break;
    case 2:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::GREATER,
                                       bin_comparator);
      break;
    case 3:
      filter_ptr = new QualifierFilter(
          CompareFilter::CompareOp::GREATER_OR_EQUAL, bin_comparator);
      break;
    case 4:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::LESS,
                                       bin_comparator);
      break;
    case 5:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::LESS_OR_EQUAL,
                                       bin_comparator);
      break;
    case 6:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::NOT_EQUAL,
                                       bin_comparator);
      break;
    case 7:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::NO_OP,
                                       bin_comparator);
      break;
    case 8:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::EQUAL,
                                       substr_comparator);
      break;
    case 9:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::GREATER,
                                       substr_comparator);
      break;
    case 10:
      filter_ptr = new QualifierFilter(
          CompareFilter::CompareOp::GREATER_OR_EQUAL, substr_comparator);
      break;
    case 11:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::LESS,
                                       substr_comparator);
      break;
    case 12:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::LESS_OR_EQUAL,
                                       substr_comparator);
      break;
    case 13:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::NOT_EQUAL,
                                       substr_comparator);
      break;
    case 14:
      filter_ptr = new QualifierFilter(CompareFilter::CompareOp::NO_OP,
                                       substr_comparator);
      break;
  }
  return filter_ptr;
}

ValueFilter *GetValueFilter(const int &filter_num, const BYTE_ARRAY &get_value,
                            const std::string &string) {

  ValueFilter *filter_ptr = nullptr;
  BinaryComparator *bin_comparator = new BinaryComparator(get_value);
  SubstringComparator *substr_comparator = new SubstringComparator(string);

  switch (filter_num) {
    case 1:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::EQUAL,
                                   bin_comparator);
      break;
    case 2:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::GREATER,
                                   bin_comparator);
      break;
    case 3:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::GREATER_OR_EQUAL,
                                   bin_comparator);
      break;
    case 4:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::LESS,
                                   bin_comparator);
      break;
    case 5:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::LESS_OR_EQUAL,
                                   bin_comparator);
      break;
    case 6:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::NOT_EQUAL,
                                   bin_comparator);
      break;
    case 7:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::NO_OP,
                                   bin_comparator);
      break;
    case 8:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::EQUAL,
                                   substr_comparator);
      break;
    case 9:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::GREATER,
                                   substr_comparator);
      break;
    case 10:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::GREATER_OR_EQUAL,
                                   substr_comparator);
      break;
    case 11:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::LESS,
                                   substr_comparator);
      break;
    case 12:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::LESS_OR_EQUAL,
                                   substr_comparator);
      break;
    case 13:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::NOT_EQUAL,
                                   substr_comparator);
      break;
    case 14:
      filter_ptr = new ValueFilter(CompareFilter::CompareOp::NO_OP,
                                   substr_comparator);
      break;
  }
  return filter_ptr;
}

RowFilter *GetRowFilter(const int &filter_num, const BYTE_ARRAY &get_row,
                        const std::string &string) {

  RowFilter *filter_ptr = nullptr;
  BinaryComparator *bin_comparator = new BinaryComparator(get_row);
  SubstringComparator *substr_comparator = new SubstringComparator(string);

  switch (filter_num) {
    case 1:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::EQUAL,
                                 bin_comparator);
      break;
    case 2:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::GREATER,
                                 bin_comparator);
      break;
    case 3:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::GREATER_OR_EQUAL,
                                 bin_comparator);
      break;
    case 4:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::LESS,
                                 bin_comparator);
      break;
    case 5:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::LESS_OR_EQUAL,
                                 bin_comparator);
      break;
    case 6:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::NOT_EQUAL,
                                 bin_comparator);
      break;
    case 7:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::NO_OP,
                                 bin_comparator);
      break;
    case 8:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::EQUAL,
                                 substr_comparator);
      break;
    case 9:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::GREATER,
                                 substr_comparator);
      break;
    case 10:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::GREATER_OR_EQUAL,
                                 substr_comparator);
      break;
    case 11:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::LESS,
                                 substr_comparator);
      break;
    case 12:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::LESS_OR_EQUAL,
                                 substr_comparator);
      break;
    case 13:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::NOT_EQUAL,
                                 substr_comparator);
      break;
    case 14:
      filter_ptr = new RowFilter(CompareFilter::CompareOp::NO_OP,
                                 substr_comparator);
      break;
  }
  return filter_ptr;
}

void TestGet(Connection &connection, std::unique_ptr<Table> &table,
             const BYTE_ARRAY &rowKey) {

  Get get(rowKey);
  BYTE_ARRAY get_family;
  BYTE_ARRAY get_family3;
  BYTE_ARRAY get_someotherfamily;
  BYTE_ARRAY get_qual;
  BYTE_ARRAY null_bytes;

  Bytes::ToBytes(GET_COLFAMILY, get_family);
  Bytes::ToBytes(GET_COLQUALIFIER, get_qual);
  Bytes::ToBytes(GET_COLFAMILY3, get_family3);
  Bytes::ToBytes(SOMEOTHERFAMILY, get_someotherfamily);

  get.AddFamily(get_family);
  get.SetMaxVersions(2);

  std::unique_ptr<Result> result(table->get(get));

  if (nullptr != result.get()) {
    LOG(INFO)<< "listCells for table: " << table->getName()->GetName()
    << std::endl;
    std::vector < Cell > cells = result->ListCells();
    for (uint i = 0; i < cells.size(); i++) {
      cells[i].Display();
    }
    cells.clear();
  }
}

void TestGets(Connection &connection, std::unique_ptr<Table> &table,
              std::vector<std::string> &rowKeys) {

  std::vector<Get *> gets;
  for (auto row : rowKeys) {
    BYTE_ARRAY rowKey = Bytes::ToBytes(row);
    Get *get = new Get(rowKey);
    BYTE_ARRAY get_family;
    BYTE_ARRAY get_family3;
    BYTE_ARRAY get_someotherfamily;
    BYTE_ARRAY get_qual;
    BYTE_ARRAY null_bytes;

    Bytes::ToBytes(GET_COLFAMILY, get_family);
    Bytes::ToBytes(GET_COLQUALIFIER, get_qual);
    Bytes::ToBytes(GET_COLFAMILY3, get_family3);
    Bytes::ToBytes(SOMEOTHERFAMILY, get_someotherfamily);

    get->AddFamily(get_family);
    gets.push_back(get);
  }

  std::vector<Result *> list_results;
  list_results = table->get(gets);

  for (auto result : list_results) {
    LOG(INFO)<< "[" << __func__ << "]" << "listCells for table: "
    << table->getName()->GetName() << std::endl;
    std::vector < Cell > cells = result->ListCells();
    for (auto cell : cells) {
      cell.Display();
    }
    cells.clear();
  }

  // Responsiblity of Caller to free the vector
  for (auto result : list_results) {
    delete result;
  }
  list_results.clear();

  for (auto &op : gets) {
    if (nullptr != op) {
      delete op;
      op = nullptr;
    }
  }
  gets.clear();
}

void TestDelete(Connection &connection, std::unique_ptr<Table> &table,
                const BYTE_ARRAY &rowKey, const KeyValue::KEY_TYPE &delete_type,
                const long &ts = 0) {

  BYTE_ARRAY get_family;
  BYTE_ARRAY get_qual;
  BYTE_ARRAY null_bytes;

  Bytes::ToBytes(GET_COLFAMILY, get_family);
  Bytes::ToBytes(GET_COLQUALIFIER, get_qual);
  Delete deleteObj(rowKey);

  switch (delete_type) {
    case KeyValue::KEY_TYPE::Delete:
      if (ts > 0) {
        deleteObj.AddColumn(get_family, get_qual, ts);
      } else {
        deleteObj.AddColumn(get_family, get_qual);
      }
      break;
    case KeyValue::KEY_TYPE::DeleteColumn:
      if (ts > 0) {
        deleteObj.AddColumns(get_family, get_qual, ts);
      } else {
        deleteObj.AddColumns(get_family, get_qual);
      }
      break;
    case KeyValue::KEY_TYPE::DeleteFamily:
      if (ts > 0) {
        deleteObj.AddFamily(get_family, ts);
      } else {
        deleteObj.AddFamily(get_family);
      }
      break;
    case KeyValue::KEY_TYPE::DeleteFamilyVersion:
      if (ts > 0) {
        deleteObj.AddFamilyVersion(get_family, ts);
      }
      break;
    default:
      break;
  }
  deleteObj.Display();
  if (table->deleteRow(deleteObj)) {
    LOG(INFO)<< "[" << __func__ << "]" << "Delete Successful for : "
    << deleteObj.GetRowAsString() << " in " << table->getName()->GetName();
  } else {
    LOG(INFO) << "[" << __func__ << "]" << "Delete Unsuccessful for : "
    << deleteObj.GetRowAsString() << " in " << table->getName()->GetName();
  }
}

void TestDeletes(Connection &connection, std::unique_ptr<Table> &table,
                 std::vector<std::string>&rowKeys) {

  std::vector<Delete*> deletes;
  for (auto row : rowKeys) {
    DLOG(INFO)<< "[" << __func__ << "]" << "Row is " << row;
    BYTE_ARRAY rowKey = Bytes::ToBytes(row);
    Delete *deleteObj = new Delete(rowKey);
    deletes.push_back(deleteObj);
  }
  if (table->deleteRow(deletes)) {
    LOG(INFO)<< "[" << __func__ << "]" << "Multi-Delete Successful"
    << std::endl;
  } else {
    LOG(INFO) << "[" << __func__ << "]" << "Multi-Delete Unsuccessful"
    << std::endl;
  }
  for (auto &op : deletes) {
    if (nullptr != op) {
      delete op;
      op = nullptr;
    }
  }
  deletes.clear();
}

void TestEnableTable(Connection &connection, std::unique_ptr<Admin> &admin,
                     const std::string &tableName) {

  admin->EnableTable(tableName);
}

void TestDisableTable(Connection &connection, std::unique_ptr<Admin> &admin,
                      const std::string &tableName) {

  admin->DisableTable(tableName);
}

void TestCreateTable(Connection &connection, std::unique_ptr<Admin> &admin,
                     const std::string &tableName) {

  std::vector<std::string> column_families;
  column_families.push_back(GET_COLFAMILY);
  column_families.push_back("colfam2");
  column_families.push_back("colfam3");
  column_families.push_back("colfam4");
  column_families.push_back(SOMEOTHERFAMILY);

  TableName *table_name = TableName::ValueOf(tableName);
  TableSchema table_schema(table_name);
  for (unsigned int num = 0; num < column_families.size(); num++) {
    ColumnFamilySchema *column_family_schema = new ColumnFamilySchema(
        column_families[num]);
    column_family_schema->SetMaxVersions(2);
    table_schema.AddFamily(*column_family_schema);
  }

  admin->CreateTable(table_schema);
}

void TestDeleteTable(Connection &connection, std::unique_ptr<Admin> &admin,
                     const std::string &tableName) {

  admin->DeleteTable(tableName);
}

void TestListTables(Connection &connection, std::unique_ptr<Admin> &admin,
                    const std::string &table_name_to_be_listed) {

  bool passed_show_table_values = false;
  if (table_name_to_be_listed.size() > 0)
    passed_show_table_values = true;

  std::vector<TableSchema> hbaseTableList;
  admin->ListTables(hbaseTableList);

  std::stringstream list_tables_str;
  list_tables_str.str("");
  for (unsigned int i = 0; i < hbaseTableList.size(); i++) {
    std::string table_list_name = hbaseTableList[i].GetTableName().GetName();
    if ((passed_show_table_values)
        && (table_name_to_be_listed != table_list_name)) {
      continue;
    }
    list_tables_str << "\n[" << table_list_name << "==>";
    const std::map<Bytes*, Bytes*> &table_metadata = hbaseTableList[i]
        .GetMetadata();

    list_tables_str << "{";
    for (const auto & table_metadata_values : table_metadata) {
      list_tables_str << Bytes::ToString(table_metadata_values.first->Get())
                      << "::";
      list_tables_str << Bytes::ToString(table_metadata_values.second->Get())
                      << "|";
    }
    list_tables_str << "}\n";
    const std::map<BYTE_ARRAY, ColumnFamilySchema> &family_map =
        hbaseTableList[i].GetFamily();
    for (const auto &col_family : family_map) {
      if (col_family.second.GetName().size() > 0)
        list_tables_str << "[ " << col_family.second.GetNameAsString() << "==>";

      list_tables_str << "{";
      const std::map<Bytes*, Bytes*> &col_family_metadata = col_family.second
          .GetValues();
      for (const auto & col_family_values : col_family_metadata) {
        list_tables_str << Bytes::ToString(col_family_values.first->Get())
                        << "::";
        list_tables_str << Bytes::ToString(col_family_values.second->Get())
                        << "|";
      }
      list_tables_str << "};{";
      const std::map<std::string, std::string> &col_family_config = col_family
          .second.GetConfiguration();
      for (const auto &config_map : col_family_config) {
        list_tables_str << config_map.first << "::" << config_map.second << "|";
      }
      list_tables_str << "}]\n";
    }

    const std::map<std::string, std::string> &table_config_map =
        hbaseTableList[i].GetConfiguration();
    for (const auto &config_map : table_config_map) {
      list_tables_str << config_map.first << "::" << config_map.second;
    }

    list_tables_str << "]";
  }

  hbaseTableList.clear();

  LOG(INFO)<< "[" << __func__ << "]" << "Table Schema: "
  << list_tables_str.str();
}

void TestIsEnabledTable(Connection &connection, std::unique_ptr<Admin> &admin,
                        const std::string &tableName) {

  admin->IsEnabledTable(tableName);
}

int CreateKeyValue(BYTE_ARRAY &bytes) {

  BYTE_ARRAY rowbytes;
  Bytes::ToBytes("row", rowbytes);

  BYTE_ARRAY fambytes;
  Bytes::ToBytes("family", fambytes);

  BYTE_ARRAY qualbytes;
  Bytes::ToBytes("qualifier", qualbytes);

  BYTE_ARRAY valbytes;
  Bytes::ToBytes("value", valbytes);

  std::vector<Tag> tags;

  BYTE_TYPE key_type = static_cast<BYTE_TYPE>(KeyValue::KEY_TYPE::Maximum);

  int bytes_size = KeyValue::CreateByteArray(
      bytes, rowbytes, 0, rowbytes.size(), fambytes, 0, fambytes.size(),
      qualbytes, 0, qualbytes.size(),
      HBaseConstants::HConstants::LATEST_TIMESTAMP, key_type, valbytes, 0,
      valbytes.size(), tags);

  KeyValue *key_value = nullptr;
  key_value = new KeyValue(rowbytes, fambytes, qualbytes,
                           HBaseConstants::HConstants::LATEST_TIMESTAMP,
                           valbytes);
  BYTE_ARRAY tmp_rowbytes;
  Bytes::CopyByteArray(key_value->GetRowArray(), tmp_rowbytes,
                       key_value->GetRowOffset(), key_value->GetRowLength());

  BYTE_ARRAY tmp_fambytes;
  Bytes::CopyByteArray(key_value->GetFamilyArray(), tmp_fambytes,
                       key_value->GetFamilyOffset(),
                       key_value->GetFamilyLength());

  BYTE_ARRAY tmp_qualbytes;
  Bytes::CopyByteArray(key_value->GetQualifierArray(), tmp_qualbytes,
                       key_value->GetQualifierOffset(),
                       key_value->GetQualifierLength());

  BYTE_ARRAY tmp_valbytes;
  Bytes::CopyByteArray(key_value->GetValueArray(), tmp_valbytes,
                       key_value->GetValueOffset(),
                       key_value->GetValueLength());

  Bytes::DisplayBytes(tmp_rowbytes);
  Bytes::DisplayBytes(tmp_fambytes);
  Bytes::DisplayBytes(tmp_qualbytes);
  DLOG(INFO)<< "Timestamp " << key_value->GetTimestamp();
  DLOG(INFO)<< "Type " << static_cast<unsigned int>(key_value->GetTypeByte());
  Bytes::DisplayBytes(tmp_valbytes);

  return bytes_size;
}

void ExtractPutKeyValues(const KeyValue *key_value) {

  BYTE_ARRAY tmp_rowbytes;
  Bytes::CopyByteArray(key_value->GetRowArray(), tmp_rowbytes,
                       key_value->GetRowOffset(), key_value->GetRowLength());

  BYTE_ARRAY tmp_fambytes;
  Bytes::CopyByteArray(key_value->GetFamilyArray(), tmp_fambytes,
                       key_value->GetFamilyOffset(),
                       key_value->GetFamilyLength());

  BYTE_ARRAY tmp_qualbytes;
  Bytes::CopyByteArray(key_value->GetQualifierArray(), tmp_qualbytes,
                       key_value->GetQualifierOffset(),
                       key_value->GetQualifierLength());

  BYTE_ARRAY tmp_valbytes;
  Bytes::CopyByteArray(key_value->GetValueArray(), tmp_valbytes,
                       key_value->GetValueOffset(),
                       key_value->GetValueLength());

  Bytes::DisplayBytes(tmp_rowbytes);
  Bytes::DisplayBytes(tmp_fambytes);
  Bytes::DisplayBytes(tmp_qualbytes);
  DLOG(INFO)<< "Timestamp " << key_value->GetTimestamp();
  DLOG(INFO)<< "Type " << static_cast<unsigned int>(key_value->GetTypeByte());
  Bytes::DisplayBytes(tmp_valbytes);

}

void ParseDetailsFromKeyValue(BYTE_ARRAY &bytes) {

  Bytes::DisplayBytes(bytes);
  DLOG(INFO)<< "bytes.size()" << bytes.size();

  int offset = 0;
  char *pCurrent = &bytes[offset];
  unsigned int *pSize = (unsigned int*) pCurrent;
  unsigned int key_length = *pSize;
  CommonUtils::SwapByteOrder(key_length);
  offset += Bytes::SIZEOF_INT;
  pCurrent += Bytes::SIZEOF_INT;

  pSize = (unsigned int*) pCurrent;
  unsigned int value_length = *pSize;
  CommonUtils::SwapByteOrder(value_length);
  offset += Bytes::SIZEOF_INT;
  pCurrent += Bytes::SIZEOF_INT;

  unsigned short *pRowLength = (unsigned short *) &bytes[offset];
  unsigned short row_length = *pRowLength;
  CommonUtils::SwapByteOrder2Bytes(row_length);
  offset += Bytes::SIZEOF_SHORT;
  pCurrent += Bytes::SIZEOF_SHORT;

  BYTE_ARRAY row;
  row.resize(row_length);
  Bytes::PutBytes(row, 0, bytes, offset, row_length);
  Bytes::DisplayBytes(row);
  offset += row_length;
  pCurrent += row_length;

  unsigned char column_family_length = bytes[offset];  //1 byte
  offset += Bytes::SIZEOF_BYTE;
  pCurrent += Bytes::SIZEOF_BYTE;

  BYTE_ARRAY column;
  column.resize(static_cast<int>(column_family_length));
  Bytes::PutBytes(column, 0, bytes, offset, column_family_length);
  Bytes::DisplayBytes(column);
  offset += column_family_length;
  pCurrent += column_family_length;

  int family_length_offset = KeyValue::ROW_KEY_OFFSET + row_length;
  int column_family_offset = family_length_offset + Bytes::SIZEOF_BYTE;
  int column_qualifier_offset = column_family_offset + column_family_length;
  int column_qualifier_length = key_length
      - (row_length + column_family_length + KeyValue::KEY_INFRASTRUCTURE_SIZE);
  int timestamp_offset = column_qualifier_offset + column_qualifier_length;

  BYTE_ARRAY column_qualifier;
  column_qualifier.resize(column_qualifier_length);
  Bytes::PutBytes(column_qualifier, 0, bytes, column_qualifier_offset,
                  column_qualifier_length);
  Bytes::DisplayBytes(column_qualifier);
  offset += column_qualifier_length;
  pCurrent += column_qualifier_length;

  /*PocoXXXX::UInt64 *pts = (PocoXXXX::UInt64*) pCurrent;
   unsigned long timestamp = *pts;
   timestamp = PocoXXXX::ByteOrder::flipBytes(timestamp);
   */
  ///////////
  unsigned long timestamp = 0L;
  for (int i = offset, j = Bytes::SIZEOF_LONG - 1; i < Bytes::SIZEOF_LONG; i++, j--) {
    timestamp <<= 8;
    timestamp ^= bytes[i] & 0xFF;
  }
  ///////////

  offset += Bytes::SIZEOF_LONG;
  pCurrent += offset;
  DLOG(INFO)<< "[" << __func__ << "]" << "Timestamp[" << timestamp << "]";

  unsigned char key_type = static_cast<unsigned char>(bytes[offset]);  //1 byte
  offset += Bytes::SIZEOF_BYTE;
  pCurrent += Bytes::SIZEOF_BYTE;
  DLOG(INFO)<< "key_type[" << static_cast<unsigned int>(key_type) << "]";

  BYTE_ARRAY value;
  value.resize(value_length);
  Bytes::PutBytes(value, 0, bytes, offset, value_length);
  Bytes::DisplayBytes(value);
  offset += value_length;
  pCurrent += value_length;

  DLOG(INFO)<< "key_length:- " << key_length;
  DLOG(INFO)<< "value_length:- " << value_length;
  DLOG(INFO)<< "row_length:- " << row_length;
  DLOG(INFO)<< "column_family_length:- " << column_family_length;
  DLOG(INFO)<< "column_qualifier_length:- " << column_qualifier_length;

  DLOG(INFO)<< "ROW_KEY_OFFSET:- " << KeyValue::ROW_KEY_OFFSET;
  DLOG(INFO)<< "family_length_offset:- " << family_length_offset;
  DLOG(INFO)<< "column_family_offset:- " << column_family_offset;
  DLOG(INFO)<< "column_qualifier_offset:- " << column_qualifier_offset;
  DLOG(INFO)<< "timestamp_offset:- " << timestamp_offset;
}

void TestMetaCache(Connection *conn) {
  std::string tableName("testtable-mr");

  std::unique_ptr<Table> table(conn->GetTable(TableName::ValueOf(tableName)));

  std::string rowKey("row-11225");
  BYTE_ARRAY row;
  Bytes::ToBytes(rowKey, row);
  TestGet(*conn, table, row);
  row.clear();

  rowKey = "row-11223";
  Bytes::ToBytes(rowKey, row);
  TestGet(*conn, table, row);
  row.clear();

  rowKey = "row-11224";
  Bytes::ToBytes(rowKey, row);
  TestGet(*conn, table, row);
  row.clear();

  rowKey = "row-124523";
  Bytes::ToBytes(rowKey, row);
  TestGet(*conn, table, row);
  row.clear();

  table->close();
}

void ReleaseConnection(Connection *conn) {

  if (conn) {
    if (!conn->IsClosed())
      conn->Close();

    delete conn;
    conn = nullptr;
  }
}

void TestScanAddfamily(std::unique_ptr<Table> &table) {

  BYTE_ARRAY family_;
  Bytes::ToBytes("colfam1", family_);
  ResultScanner *scanner = table->getScanner(family_);
  Result *result = nullptr;

  while (!scanner->close_scanner_) {
    result = scanner->Next();
    if (result != NULL) {
      Result *pResult = result;
      std::vector<Cell> cells = pResult->ListCells();
      for (auto cell : cells) {
        cell.Display();
      }
      cells.clear();
    }
  }
}

void TestScanAddColumn(std::unique_ptr<Table> &table) {

  BYTE_ARRAY family_;
  Bytes::ToBytes("colfam1", family_);
  BYTE_ARRAY qual_;
  Bytes::ToBytes("col-1", qual_);
  ResultScanner *scanner = table->getScanner(family_, qual_);
  Result *result = nullptr;

  while (!scanner->close_scanner_) {
    result = scanner->Next();
    if (result != NULL) {
      Result *pResult = result;
      std::vector<Cell> cells = pResult->ListCells();
      for (auto cell : cells) {
        cell.Display();
      }
      cells.clear();
    }
  }
}

void ScanResult(ResultScanner *scanner, std::unique_ptr<Table> &table) {
  Result *result = nullptr;
  while (!scanner->close_scanner_) {
    result = scanner->Next();
    if (result != NULL) {
      Result *pResult = result;
      LOG(INFO)<< "[" << __func__ << "]" << "listCells for row: "
      << (nullptr != result->GetRow() ? *result->GetRow() : "")
      << " in table: " << table->getName()->GetName() << std::endl;
      std::vector<Cell> cells = pResult->ListCells();
      for (auto cell : cells) {
        cell.Display();
      }
      cells.clear();
      delete result;
    }
  }
}

void TestScan(std::unique_ptr<Table> &table) {

  Scan scan;

  std::string start_row = "";
  std::string end_row = "";

  scan.SetStartRowVal(start_row);
  scan.SetStopRowVal(end_row);
  int caching = 100;
  scan.SetCaching(caching);

  ResultScanner *scanner = table->getScanner(scan);

  ScanResult(scanner, table);
  delete scanner;
}

void TestScanAddfamily(std::unique_ptr<Table> &table, BYTE_ARRAY family_) {

  ResultScanner *scanner = table->getScanner(family_);
  ScanResult(scanner, table);
  delete scanner;
}

void TestScanAddColumn(std::unique_ptr<Table> &table, BYTE_ARRAY family_,
                       BYTE_ARRAY qual_) {

  ResultScanner *scanner = table->getScanner(family_, qual_);
  ScanResult(scanner, table);
  delete scanner;
}

void TestScanStartrow(std::unique_ptr<Table> &table, std::string start_row) {

  Scan scan;

  scan.SetStartRowVal(start_row);
  int caching = 100;
  scan.SetCaching(caching);

  ResultScanner *scanner = table->getScanner(scan);
  ScanResult(scanner, table);
  delete scanner;
}

void TestScanStoprow(std::unique_ptr<Table> &table, std::string start_row,
                     std::string end_row) {

  Scan scan;

  scan.SetStartRowVal(start_row);
  scan.SetStopRowVal(end_row);
  int caching = 100;
  scan.SetCaching(caching);

  ResultScanner *scanner = table->getScanner(scan);
  ScanResult(scanner, table);
  delete scanner;
}

void TestScanReversed(std::unique_ptr<Table> &table) {

  Scan scan;

  std::string start_row = "";
  std::string end_row = "";
  bool val = true;
  scan.SetReversed(val);
  scan.SetSmall(val);
  scan.SetStartRowVal(start_row);
  scan.SetStopRowVal(end_row);
  int caching = 100;
  scan.SetCaching(caching);

  ResultScanner *scanner = table->getScanner(scan);
  ScanResult(scanner, table);
  delete scanner;
}
