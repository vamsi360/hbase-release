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
#include <string>
#include <vector>


void SwapByteOrder(unsigned int &ui);
void GetTableAndNamespace(const std::string &tableName, std::string &nameSpaceValue,
    std::string &tableNameValue);
void SwapByteOrder2Bytes(unsigned short &us);
void Tokenize(const std::string &in, const std::string &delimiter, std::vector<std::string>&out);

