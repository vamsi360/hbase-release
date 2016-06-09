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

#include "cell.h"

class CellScanner {

 public:
  CellScanner();
  CellScanner(const CellScanner& cell_scanner);
  virtual ~CellScanner();
  void SetData(char *cell_block_data, int data_length);
  char *GetData() const;
  int GetDataLength() const;
  bool Advance();
  Cell *Current();

 private:
  char *cell_block_data_;
  int	data_length_;
  int cur_pos_;
  char *cur_block_;
  Cell *current_cell_;
  ByteBuffer *current_cell_data_;
  long cells_read_;
  bool beyond_block_data_;
};
