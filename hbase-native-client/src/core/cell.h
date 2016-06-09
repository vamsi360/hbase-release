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

#include <iostream>

#include "bytes.h"
#include "hconstants.h"
#include "key_value.h"

namespace CellType {
enum cellType{
	UNKNOWN = -1,
	MINIMUM = 0,
	PUT = 4,
	DELETE = 8,
	DELETE_COLUMN = 12,
	DELETE_FAMILY = 14,
	DELETE_FAMILYVERSION = 16,
	MAXIMUM = 255
} ;
}

class Cell{

 public:
  Cell();
  Cell(const std::string &row, const std::string &family, const std::string &qualifier,
		const long &timestamp, const std::string &value, const std::string &tags, const CellType::cellType &cellType);
  Cell(const Cell &cell);
  Cell& operator= (const Cell &cell);

  virtual ~Cell();
  void Display();
  const std::string &Row();
  const std::string &Family();
  const std::string &Qualifier();
  const std::string &Value();
  const CellType::cellType &TypeByte();
  const long &Timestamp();
  const KeyValue *GetKeyValue() const;
  static Cell *CreateCell(const BYTE_ARRAY &bytes);
  static Cell * Parse(ByteBuffer &cell_data);

 private:
  void DisplayKeyValueDetails();
  std::string row_;
  std::string family_;
  std::string qualifier_;
  long  timestamp_;
  std::string value_;
  std::string tags_;
  CellType::cellType cellType_;
  KeyValue *key_value_;
};
