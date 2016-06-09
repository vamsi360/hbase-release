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

#include "filter.h"

class FilterBase : public Filter {

 public:
  FilterBase();
  virtual ~FilterBase();
  static Filter *CreateFilterFromArguments();
  virtual void Reset();
  virtual bool FilterRowKey(const Cell &first_row_cell);
  virtual bool FilterAllRemaining();
  virtual bool FilterKeyValue();
  virtual void FilterRowCells(std::vector<Cell> key_values);
  virtual bool HasFilterRow();
  virtual bool FilterRow();
  virtual Cell *GetNextCellHint(const Cell &current_cell);
  virtual bool IsFamilyEssential(const BYTE_ARRAY &name);
  virtual bool AreSerializedFieldsEqual(const Filter &other);

  //! Implemenration of Pure Virtual Functions
  virtual const char *GetName() = 0;
  virtual bool ToByteArray(hbase::pb::Filter &filter) = 0;
  //  TODO
  //  abstract public ReturnCode filterKeyValue(final Cell v) throws IOException;
  //  abstract public Cell transformCell(final Cell v) throws IOException;
};
