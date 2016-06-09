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
#include "cell_scanner.h"

#include <iostream>
#include <vector>

class Result {

 public:
  Result();
  Result(const bool &readonly);
  Result (std::vector<Cell> &cells, const bool &exists, const bool &stale,
      const bool &partial);
  Result (std::vector<Cell*> *pcells, const bool &exists, const bool &stale,
      const bool &partial);
  virtual ~Result();
  const std::vector<Cell> &ListCells();
  void GetColumnCells(const std::string &family, const std::string &qualifier,
      std::vector<Cell> &columnCells);
  const std::string *GetValue(const std::string &family,
      const std::string &qualifier);
  Cell *GetColumnLatestCell(const std::string &family,
      const std::string &qualifier);
  const std::string *GetRow();
  const bool IsEmpty();

 private:
  bool empty_;
  bool exists_;
  bool stale_;
  bool partial_;
  bool readonly_;
  std::string row_;
  std::vector<Cell> cells_;
  std::vector<Cell *> *pcells_;
};
