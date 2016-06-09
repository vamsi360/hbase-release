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


#include "cell_scanner.h"
#include <glog/logging.h>
#include "utils.h"

CellScanner::CellScanner() {
  cell_block_data_ = nullptr;
  data_length_ = 0;
  cur_pos_ = 0;
  cur_block_ = nullptr;
  current_cell_ = nullptr;
  current_cell_data_ = nullptr;
  cells_read_ = 0;
  beyond_block_data_ = false;
}

CellScanner::CellScanner(const CellScanner &cell_scanner) {

  if (nullptr != cell_scanner.cell_block_data_) {
    cell_block_data_ = new char[cell_scanner.data_length_];
    for (int i = 0; i < cell_scanner.data_length_; i++)
      cell_block_data_[i] = cell_scanner.cell_block_data_[i];
  } else {
    cell_block_data_ = nullptr;
  }

  data_length_ = cell_scanner.data_length_;
  cur_pos_ = 0;
  if (nullptr != cell_block_data_) {
    cur_block_ = cell_block_data_;
  } else {
    cur_block_ = nullptr;
  }
  current_cell_ = nullptr;
  current_cell_data_ = nullptr;
  cells_read_ = 0;
  beyond_block_data_ = false;
}

CellScanner::~CellScanner() {

  if (nullptr != current_cell_data_) {
    delete current_cell_data_;
    current_cell_data_ = nullptr;
  }

  if (nullptr != current_cell_) {
    delete current_cell_;
    current_cell_ = nullptr;
  }

  if (nullptr != cell_block_data_) {
    delete [] cell_block_data_;
    cell_block_data_ = nullptr;
  }
}

void CellScanner::SetData(char *cell_block_data, int data_length) {
  cell_block_data_ = cell_block_data;
  data_length_ = data_length;
  cur_pos_ = 0;
  cur_block_ = cell_block_data_;
}

char *CellScanner::GetData() const {
  return cell_block_data_;
}

int CellScanner::GetDataLength() const {
  return data_length_;
}

bool CellScanner::Advance() {
  if (beyond_block_data_) {
    return false;
  }

  unsigned int *pSize = (unsigned int*) cur_block_;
  unsigned int cellSize = *pSize;
  CommonUtils::SwapByteOrder(cellSize);
  int total_size = cellSize + 4;

  if (nullptr != current_cell_data_) {
    delete current_cell_data_;
    current_cell_data_ = nullptr;
  }

  if (nullptr != current_cell_) {
    delete current_cell_;
    current_cell_ = nullptr;
  }

  if ((cur_pos_ + total_size) > data_length_) {
    beyond_block_data_ = true;
    return false;
  }

  current_cell_data_ = new ByteBuffer (cur_block_, cur_block_ + total_size);
  cur_block_ += total_size;
  cur_pos_ += total_size;
  cells_read_++;

  current_cell_ = Cell::Parse(*current_cell_data_);
  return true;
}

Cell *CellScanner::Current() {
  Cell *temp_cell = current_cell_;
  current_cell_ = nullptr;
  return temp_cell;
}


