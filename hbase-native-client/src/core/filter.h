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

#include <memory>

#include "../rpc/generated/Filter.pb.h"

#include "bytes.h"
#include "cell.h"

class Filter {
 public:
  enum class ReturnCode{
    /**
     * Include the Cell
     */
    INCLUDE,
    /**
     * Include the Cell and seek to the next column skipping older versions.
     */
    INCLUDE_AND_NEXT_COL,
    /**
     * Skip this Cell
     */
    SKIP,
    /**
     * Skip this column. Go to the next column in this row.
     */
    NEXT_COL,
    /**
     * Done with columns, skip to next row. Note that filterRow() will
     * still be called.
     */
    NEXT_ROW,
    /**
     * Seek to next key which is given as hint by the filter.
     */
    SEEK_NEXT_USING_HINT,
    /**
     * Include KeyValue and done with row, seek to next.
     */
    INCLUDE_AND_SEEK_NEXT_ROW,
  };

  Filter();
  Filter(const Filter&);
  Filter& operator=(const Filter &cfilter);
  virtual ~Filter();

  static Filter *ParseFrom(const BYTE_ARRAY pb_bytes);

  virtual void Reset();
  virtual bool FilterAllRemaining();
  virtual bool FilterKeyValue();
  virtual void FilterRowCells(std::vector<Cell> key_values);
  virtual bool HasFilterRow();
  virtual bool FilterRow();
  virtual Cell *GetNextCellHint(const Cell &current_cell);
  virtual bool IsFamilyEssential(const BYTE_ARRAY &name);
  virtual bool AreSerializedFieldsEqual(const Filter &other);

  void SetReversed(const bool &reversed);
  bool IsReversed();

  virtual const char *GetName() = 0;
  virtual bool ToByteArray(hbase::pb::Filter &filter) = 0;

  //  TODO
  //  abstract public Cell transformCell(final Cell v) throws IOException;
  //  abstract public ReturnCode filterKeyValue(final Cell v) throws IOException;

 protected:
  bool reversed_;
};
