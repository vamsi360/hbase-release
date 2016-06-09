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

#include "result.h"

Result::Result(): empty_(true),
exists_(false),
stale_(false),
partial_(false),
readonly_(false) {
  // TODO Auto-generated constructor stub
}

Result::~Result() {
  // TODO Auto-generated destructor stub
}


Result::Result(const bool &readonly):readonly_(readonly) {

}

Result::Result (std::vector<Cell> &cells, const bool &exists, const bool &stale, const bool &partial):
        empty_(cells.empty()),
        exists_(exists),
        stale_(stale),
        partial_(partial),
        readonly_(false) {
  this->cells_.insert (this->cells_.end(), cells.begin(), cells.end());

}

Result::Result(std::vector<Cell*> *pcells, const bool &exists,
    const bool &stale, const bool &partial) :  empty_(pcells->empty()),
        exists_(exists),
        stale_(stale),
        partial_(partial),
        readonly_(false) {
  pcells_ = pcells;
}

const std::vector<Cell> &Result::ListCells() {
  return this->cells_;
}

void Result::GetColumnCells(const std::string &family, const std::string &qualifier, std::vector<Cell> &columnCells) {

  for (unsigned int i = 0; i < this->cells_.size(); i++) {
    if (cells_[i].Family() == family && cells_[i].Qualifier() == qualifier)
      columnCells.push_back(cells_[i]);
  }
  return;
}

const std::string *Result::GetValue(const std::string &family, const std::string &qualifier) {

  std::string *result = NULL;
  for (unsigned int i = 0; i < this->cells_.size(); i++) {
    if (this->cells_[i].Family() == family && this->cells_[i].Qualifier() == qualifier)
      result = new std::string(this->cells_[i].Value());
  }
  return result;
}

Cell *Result::GetColumnLatestCell(const std::string &family, const std::string &qualifier) {

  Cell *cell = NULL;
  if (this->IsEmpty())
    cell = new Cell();
  else {
    for(unsigned int i = 0; i < this->cells_.size(); i++){
      if(this->cells_[i].Family() == family && this->cells_[i].Qualifier() == qualifier)
        cell = new Cell(this->cells_[i]);
    }
  }
  return cell;
}


const std::string *Result::GetRow( ){

  std::string *result(nullptr);
  std::string cell_row("");
  cell_row = this->cells_[0].Row();
  if ("" == this->row_ || 0 == this->row_.length()) {
    this->row_ = (0 == this->cells_.size()) ? "" : this->cells_[0].Row();
  }
  result = new std::string(this->row_);
  return result;
}

const bool Result::IsEmpty() {

  return this->empty_;
}
