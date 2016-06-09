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

#include "time_range.h"
#include "hconstants.h"

TimeRange::TimeRange():minTimeStamp_(0L), maxTimeStamp_(std::numeric_limits< long >::max()), allTime_(true){

}

TimeRange::~TimeRange() {

}

TimeRange::TimeRange(const long &minTimeStamp){

  this->minTimeStamp_ = minTimeStamp;
  this->maxTimeStamp_ = std::numeric_limits< long >::max();
  this->allTime_ = false;
}

TimeRange::TimeRange(const long &minTimeStamp, const long &maxTimeStamp){

  if (minTimeStamp < 0 || maxTimeStamp < 0) {
    throw std::string("Timestamp cannot be negative.");// minStamp:" + minTimeStamp + ", maxStamp:" + maxTimeStamp);
  }
  if(maxTimeStamp < minTimeStamp) {
    throw std::string("maxStamp is smaller than minStamp");
  }

  this->minTimeStamp_ = minTimeStamp;
  this->maxTimeStamp_ = maxTimeStamp;
  this->allTime_ = false;
}

const long& TimeRange::GetMin() const {
  return this->minTimeStamp_;

}

const long& TimeRange::GetMax() const {
  return this->maxTimeStamp_;
}

const bool& TimeRange::GetAllTime() const{
  return this->allTime_;
}
