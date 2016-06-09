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

#include "filter_base.h"
#include "return_codes.h"

Filter *FilterBase::CreateFilterFromArguments() {

  Filter *filter = NULL;
  throw UNDEFINED_API;
  return filter;
}

FilterBase::FilterBase() {
  // TODO Auto-generated constructor stub

}

FilterBase::~FilterBase() {
  // TODO Auto-generated destructor stub
}

void FilterBase::Reset() {

  return;
}

bool FilterBase::FilterRowKey(const Cell &first_row_cell) {
  return false;
}

bool FilterBase::FilterAllRemaining() {

  return false;
}

bool FilterBase::FilterKeyValue() {

  return true;
}

void FilterBase::FilterBase::FilterRowCells(std::vector<Cell> key_values) {

  return;
}

bool FilterBase::HasFilterRow() {

  return false;
}

bool FilterBase::FilterRow() {

  return false;
}

Cell *FilterBase::GetNextCellHint(const Cell &current_cell) {

  Cell *cell = NULL;
  return cell;
}

bool FilterBase::IsFamilyEssential(const BYTE_ARRAY &name) {

  return true;
}

bool FilterBase::AreSerializedFieldsEqual(const Filter &other) {

  return true;
}

const char *FilterBase::GetName() {

  throw "FilterBase::GetName() Undefined\n";
}

bool FilterBase::ToByteArray(hbase::pb::Filter &filter) {

  throw "FilterBase::ToByteArray() Undefined\n";
}
