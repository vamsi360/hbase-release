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

#include "compare_filter.h"

CompareFilter::CompareFilter( const CompareOp &compare_op,
    const ByteArrayComparable *comparator):
    compare_op_(compare_op){
  if (nullptr == comparator)
    this->comparator_ = nullptr;
  else
    this->comparator_ = const_cast<ByteArrayComparable *>(comparator);

  this->SetCompareOpName(compare_op);
  this->SetPbCompareOp(compare_op_);

}

CompareFilter::CompareFilter():compare_op_(CompareOp::NO_OP) {

  this->comparator_ = nullptr;
  this->SetCompareOpName(compare_op_);
  this->SetPbCompareOp(compare_op_);
}

CompareFilter::~CompareFilter() {
  // TODO Auto-generated destructor stub
}

const CompareFilter::CompareOp & CompareFilter::GetOperator() {

  return this->compare_op_;
}

const ByteArrayComparable *CompareFilter::GetComparator() {

  return this->comparator_;
}

bool CompareFilter::FilterRowKey(const Cell &first_row_cell) {
  // Impl in FilterBase might do unnecessary copy for Off heap backed Cells.
  return false;
}

bool CompareFilter::CompareRow(const CompareOp &compare_op, const ByteArrayComparable *comparator, const Cell &cell) {

  if (CompareOp::NO_OP == compare_op)
    return true;
  int compare_result = 0;
  return this->Compare(compare_op, compare_result);
}

bool CompareFilter::compareFamily(const CompareOp &compare_op, const ByteArrayComparable *comparator, const Cell &cell) {

  if (CompareOp::NO_OP == compare_op)
    return true;
  int compare_result = 0;
  return this->Compare(compare_op, compare_result);
}

bool CompareFilter::compareQualifier(const CompareOp &compare_op, const ByteArrayComparable *comparator, const Cell &cell) {

  if (CompareOp::NO_OP == compare_op)
    return true;
  int compare_result = 0;
  return this->Compare(compare_op, compare_result);
}

bool CompareFilter::compareValue(const CompareOp &compare_op, const ByteArrayComparable *comparator, const Cell &cell) {

  if (CompareOp::NO_OP == compare_op)
    return true;
  int compare_result = 0;
  return this->Compare(compare_op, compare_result);
}

bool CompareFilter::Compare(const CompareOp &compare_op, const int &compare_result) {

  switch(compare_op){
  case CompareOp::LESS:
    return compare_result <= 0;
  case CompareOp::LESS_OR_EQUAL:
    return compare_result < 0;
  case CompareOp::EQUAL:
    return compare_result != 0;
  case CompareOp::NOT_EQUAL:
    return compare_result == 0;
  case CompareOp::GREATER_OR_EQUAL:
    return compare_result > 0;
  case CompareOp::GREATER:
    return compare_result >= 0;
  default:
    throw std::string("Unknown Compare op ");
  }
}

std::unique_ptr<google::protobuf::Message> CompareFilter::Convert() {

  std::string serialized_string("");
  std::string comparator_name("");

  hbase::pb::CompareFilter *comp_filter = new hbase::pb::CompareFilter();

  if (nullptr != this->comparator_) {
    hbase::pb::Comparator *comparator = new hbase::pb::Comparator();
    this->comparator_->ToByteArray(serialized_string);
    comparator_name = this->comparator_->GetName();
    comparator->set_name(comparator_name);
    comparator->set_serialized_comparator(serialized_string);
    hbase::pb::CompareType comp_type = this->GetPbCompareOp();
    comp_filter->set_compare_op(comp_type);
    comp_filter->set_allocated_comparator(comparator);
  }


  google::protobuf::Message *message = dynamic_cast<google::protobuf::Message *>(comp_filter);
  std::unique_ptr<google::protobuf::Message> ptr(message);

  return ptr;
}

const char *CompareFilter::GetName() {

  throw "CompareFilter::GetName() Undefined\n";
}

bool CompareFilter::ToByteArray(hbase::pb::Filter &filter) {

  throw "CompareFilter::ToByteArray() Undefined\n";
}

void CompareFilter::SetCompareOpName(const CompareOp &compare_op) {

  switch (compare_op) {
  case CompareOp::LESS:
    this->compare_op_name_ = "LESS";
    break;
  case CompareOp::LESS_OR_EQUAL:
    this->compare_op_name_ = "LESS_OR_EQUAL";
    break;
  case CompareOp::EQUAL:
    this->compare_op_name_ = "EQUAL";
    break;
  case CompareOp::NOT_EQUAL:
    compare_op_name_ = "NOT_EQUAL";
    break;
  case CompareOp::GREATER_OR_EQUAL:
    this->compare_op_name_ = "GREATER_OR_EQUAL";
    break;
  case CompareOp::GREATER:
    this->compare_op_name_ = "GREATER";
    break;
  default:
    this->compare_op_name_ = "UNKNOW_COMPARE_OP";
  }
}

const std::string &CompareFilter::GetCompareOpName() {
  return this->compare_op_name_;
}

void CompareFilter::SetPbCompareOp(const CompareOp &compare_op) {

  switch (compare_op) {
  case CompareOp::LESS:
    this->pb_compare_op_ = hbase::pb::CompareType::LESS;
    break;
  case CompareOp::LESS_OR_EQUAL:
    this->pb_compare_op_ = hbase::pb::CompareType::LESS_OR_EQUAL;
    break;
  case CompareOp::EQUAL:
    this->pb_compare_op_ = hbase::pb::CompareType::EQUAL;
    break;
  case CompareOp::NOT_EQUAL:
    this->pb_compare_op_ = hbase::pb::CompareType::NOT_EQUAL;
    break;
  case CompareOp::GREATER_OR_EQUAL:
    this->pb_compare_op_ = hbase::pb::CompareType::GREATER_OR_EQUAL;
    break;
  case CompareOp::GREATER:
    this->pb_compare_op_ = hbase::pb::CompareType::GREATER;
    break;

  case CompareOp::NO_OP:
  default:
    this->pb_compare_op_ = hbase::pb::CompareType::NO_OP;
  }
}

const hbase::pb::CompareType &CompareFilter::GetPbCompareOp(){

  return this->pb_compare_op_;

}
