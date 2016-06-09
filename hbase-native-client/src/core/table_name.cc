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

#include "table_name.h"

#include <sstream>
#include <glog/logging.h>
#include "exception.h"

const std::string TableName::NAMESPACE_DELIM = ":";
const std::string TableName::DEFAULT_NAMESPACE = "default";
const std::string TableName::SYSTEM_NAMESPACE = "hbase";
const std::string TableName::OLD_META_STR = ".META.";
const std::string TableName::OLD_ROOT_STR = "-ROOT-";

TableName* TableName::ValueOf(const char *table_name) {

  std::string table_name_str("");
  if (nullptr != table_name)
    table_name_str = table_name;

  return TableName::ValueOf(table_name_str);
}

TableName* TableName::ValueOf(const std::string &tableName) {

  TableName *tableNameObj = nullptr;
  size_t pos = tableName.find(NAMESPACE_DELIM, 0);
  if (std::string::npos != pos) {
    tableNameObj = CreateTableNameIfNecessary(tableName.substr(0, pos), tableName.substr(pos+1));
  } else {
    tableNameObj = CreateTableNameIfNecessary(TableName::DEFAULT_NAMESPACE, tableName);
  }
  return tableNameObj;
}

TableName* TableName::CreateTableNameIfNecessary(const std::string &nameSpace, const std::string &qualifier){

  //TODO check in tablecache as in Java if not present call construuctor and add in cache
  TableName *tableNameObj = new TableName(nameSpace, qualifier);
  return tableNameObj;
}

void TableName::IsLegalFulyQualifiedTableName(const std::string &fullyQfdTableName){
  if (0 == fullyQfdTableName.length())
    throw HBaseException("Full Qualified Table Name name must not be empty");

}

void TableName::IsLegalNameSpaceName(const std::string &nameSpace) {

  if (0 == nameSpace.length())
    throw HBaseException("Namespace name must not be empty");
  for (uint i =0; i < nameSpace.length(); i++){
    if (std::isalnum(nameSpace[i]) || '_' == nameSpace[i]) {
      continue;
    } else {
      std::stringstream str_error ;
      str_error.str("");
      str_error << "Illegal character <" << nameSpace[i] << "> at " << i
          << ". Namespaces can only contain 'alphanumeric characters': i.e. [a-zA-Z_0-9]: "
          << std::endl;
      throw HBaseException(str_error.str());

    }
  }
}

void TableName::IsLegalTableQualifierName(const std::string &qualifier) {
  if (0 == qualifier.length())
    throw HBaseException("Qualifier name must not be empty");
  if ('.' == qualifier[0] || '-' == qualifier[0]){

  }
  for (unsigned int i =0; i < qualifier.length(); i++) {
    if (std::isalnum(qualifier[i]) || '_' == qualifier[i] || '-' == qualifier[i] || '.' == qualifier[i]) {
      continue;
    } else {
      std::stringstream str_error ;
      str_error.str("");
      str_error << "Invalid character <" << qualifier[i] << "> at " << i
          << "encountered."
          << std::endl;
      throw HBaseException(str_error.str());
    }
  }
}

TableName::TableName() {

}

TableName::TableName(const TableName &ctableName) {

  DLOG(INFO)  <<"Copy Table";
  this->nameSpace_ = ctableName.nameSpace_;
  this->qualifier_ = ctableName.qualifier_;
  this->systemTable_ = ctableName.systemTable_;
  this->name_ = ctableName.name_;
}

TableName& TableName::operator= (const TableName &ctableName) {

  DLOG(INFO)  <<"Assign Table" ;
  this->nameSpace_ = ctableName.nameSpace_;
  this->qualifier_ = ctableName.qualifier_;
  this->systemTable_ = ctableName.systemTable_;
  this->name_ = ctableName.name_;

  return *this;

}

TableName::TableName(const std::string &nameSpace, const std::string &qualifier):
		                    nameSpace_(nameSpace),
		                    qualifier_(qualifier),
		                    systemTable_(false),
		                    name_("") {

  if (TableName::OLD_ROOT_STR == this->qualifier_) {
    std::stringstream str_error ;
    str_error.str("");
    str_error << TableName::OLD_ROOT_STR << " is deprecated. " << std::endl;
    throw HBaseException(str_error.str());
  }
  if (TableName::OLD_META_STR == this->qualifier_) {
    std::stringstream str_error ;
    str_error.str("");
    str_error << TableName::OLD_META_STR << " no longer exists Meta table is renamed now to. hbase:meta " << std::endl;
    throw HBaseException(str_error.str());
  }
  if (this->nameSpace_ == TableName::DEFAULT_NAMESPACE) {
    this->name_ = this->qualifier_;
  } else {
    if (this->nameSpace_ == TableName::SYSTEM_NAMESPACE) {
      this->systemTable_ = true;
    } else {
      this->name_ = this->nameSpace_;
      this->name_ += TableName::NAMESPACE_DELIM;
    }
    this->name_ += this->qualifier_;
  }
  IsLegalNameSpaceName(this->nameSpace_);
  IsLegalTableQualifierName(this->qualifier_);
}

TableName::~TableName() {
}

bool TableName::operator <(const TableName &rhs) const {

  if (this->name_ < rhs.name_ )
    return true;

  if (this->name_ > rhs.name_)
    return false;

  if (this->qualifier_ < rhs.qualifier_)
    return true;

  if (this->qualifier_ > rhs.qualifier_)
    return false;

  return false;
}

const std::string &TableName::GetName() const{

  return this->name_;
}
