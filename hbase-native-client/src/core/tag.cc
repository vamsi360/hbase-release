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

#include "tag.h"
#include <limits>

const int Tag::TYPE_LENGTH_SIZE = Bytes::SIZEOF_BYTE;
const int Tag::TAG_LENGTH_SIZE = Bytes::SIZEOF_SHORT;
const int Tag::INFRASTRUCTURE_SIZE = Tag::TYPE_LENGTH_SIZE + Tag::TAG_LENGTH_SIZE;
const int Tag::MAX_TAG_LENGTH = (2 * std::numeric_limits< short >::max()) + 1 - Tag::TAG_LENGTH_SIZE;

Tag::Tag() {
  // TODO Auto-generated constructor stub

}

Tag::~Tag() {
  // TODO Auto-generated destructor stub
}

