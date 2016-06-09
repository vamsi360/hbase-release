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

#include "byte_array_comparable.h"
#include "cell.h"
#include "filter_base.h"

class CompareFilter : public FilterBase {
 public:
  enum class CompareOp{
    /** less than */
    LESS,
    /** less than or equal to */
    LESS_OR_EQUAL,
    /** equals */
    EQUAL,
    /** not equal */
    NOT_EQUAL,
    /** greater than or equal to */
    GREATER_OR_EQUAL,
    /** greater than */
    GREATER,
    /** no operation */
    NO_OP
  };
  CompareFilter();
  CompareFilter(const CompareOp &compare_op, const ByteArrayComparable *comparator);
  const CompareFilter::CompareOp & GetOperator();
  const ByteArrayComparable *GetComparator();
  virtual ~CompareFilter();
  bool FilterRowKey(const Cell &first_row_cell);
  //  TODO
  //  public static ArrayList<Object> extractArguments(ArrayList<byte []> filterArguments)
  //  FilterProtos.CompareFilter convert()
  //  boolean areSerializedFieldsEqual(Filter o)
  //  public String toString()
  std::unique_ptr<google::protobuf::Message> Convert();
  const std::string &GetCompareOpName();
  const hbase::pb::CompareType &GetPbCompareOp();

  virtual const char *GetName() = 0;
  virtual bool ToByteArray(hbase::pb::Filter &filter) = 0;

 protected:

  bool CompareRow(const CompareOp &compare_op, const ByteArrayComparable *comparator, const Cell &cell);
  bool compareFamily(const CompareOp &compare_op, const ByteArrayComparable *comparator, const Cell &cell);
  bool compareQualifier(const CompareOp &compare_op, const ByteArrayComparable *comparator, const Cell &cell);
  bool compareValue(const CompareOp &compare_op, const ByteArrayComparable *comparator, const Cell &cell);
  CompareOp compare_op_;
  ByteArrayComparable *comparator_;

 private:
  bool Compare(const CompareOp &compare_op, const int &compare_result);
  void SetCompareOpName(const CompareOp &compare_op);
  void SetPbCompareOp(const CompareOp &compare_op);
  std::string compare_op_name_;
  hbase::pb::CompareType pb_compare_op_;
};
