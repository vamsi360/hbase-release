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

#include "filter.h"
#include <glog/logging.h>

Filter::Filter():reversed_(false) {
  // TODO Auto-generated constructor stub

}

Filter::~Filter() {
  // TODO Auto-generated destructor stub
}

Filter::Filter(const Filter &cfilter) {
  DLOG(INFO)  <<"Copy filter";
  this->reversed_ = cfilter.reversed_;
}

Filter & Filter::operator = (const Filter &cfilter) {
  DLOG(INFO)  <<"Assign filter";

  this->reversed_ = cfilter.reversed_;
  return *this;
}

Filter *Filter::ParseFrom(const BYTE_ARRAY pb_bytes) {

  Filter *filter = NULL;
  return filter;
}


void Filter::Reset() {

  return;
}

bool Filter::FilterAllRemaining() {

  return true;
}

bool Filter::FilterKeyValue() {

  return true;
}

void Filter::Filter::FilterRowCells(std::vector<Cell> key_values) {

  return;
}

bool Filter::HasFilterRow() {

  return true;
}

bool Filter::FilterRow() {

  return true;
}

Cell *Filter::GetNextCellHint(const Cell &current_cell) {

  Cell *cell = NULL;
  return cell;
}

bool Filter::IsFamilyEssential(const BYTE_ARRAY &name) {

  return true;
}

bool Filter::AreSerializedFieldsEqual(const Filter &other) {

  return true;
}

void Filter::SetReversed(const bool &reversed) {
  this->reversed_ = reversed;
  return;
}

bool Filter::IsReversed() {

  return this->reversed_;
}
