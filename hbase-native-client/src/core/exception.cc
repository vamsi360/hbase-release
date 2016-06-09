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


#include "exception.h"
#include <glog/logging.h>

const HBaseException::EXCEPTIONLIST_MAP HBaseException::EXCEPTIONS = HBaseException::GetExceptionList();

HBaseException::EXCEPTIONLIST_MAP HBaseException::GetExceptionList() {

  EXCEPTIONLIST_MAP exc_list;
  exc_list.insert(EXCEPTIONLIST_PAIR("org.apache.hadoop.hbase.NotServingRegionException:",
      HBaseException::HBASEERRCODES::ERR_REGION_SERVER_NOT_SERVING));
  exc_list.insert(EXCEPTIONLIST_PAIR("org.apache.hadoop.hbase.exceptions.RegionMovedException:",
      HBaseException::HBASEERRCODES::ERR_REGION_MOVED));
  return exc_list;
}

const HBaseException::RETRY_EXCEPTIONLIST_MAP HBaseException::RETRY_EXCEPTIONS = HBaseException::GetRetryExceptionList();

HBaseException::RETRY_EXCEPTIONLIST_MAP HBaseException::GetRetryExceptionList() {

  RETRY_EXCEPTIONLIST_MAP retry_list;
  retry_list.insert(RETRY_EXCEPTIONLIST_PAIR(HBaseException::HBASEERRCODES::ERR_REGION_SERVER_NOT_SERVING, false));
  retry_list.insert(RETRY_EXCEPTIONLIST_PAIR(HBaseException::HBASEERRCODES::ERR_REGION_MOVED, true));
  return retry_list;
}

HBaseException::HBaseException(const std::string &error, bool stack_trace): error_(error),
    stack_trace_(""),
    show_stack_trace_(false),
    retry_(false),
    err_code_(HBaseException::HBASEERRCODES::ERR_UNKNOWN),
    retry_count_(0){

  if (stack_trace) {
    this->stack_trace_ = error;
  }

  size_t pos = this->error_.find("at ");
  if (std::string::npos != pos){
    this->error_.erase(pos);
  }

  for (const auto exception : EXCEPTIONS) {
    pos = this->stack_trace_.find(exception.first);
    if (std::string::npos != pos){
      this->err_code_ = exception.second;
      try {
        this->retry_ = RETRY_EXCEPTIONS.at(this->err_code_);
      } catch(const std::out_of_range &oor) {
        //By default no retries.
        this->retry_ = false;
        DLOG(WARNING)  << "Can't find definition of the occured exception. No retries will ever occur:- " << this->stack_trace_;
      }

      break;
    }

  }

  this->error_ += "\n";
  this->stack_trace_ += "\n";
}

HBaseException::~HBaseException() {
  // TODO Auto-generated destructor stub
}

const char *HBaseException::what() const throw() {

  if (ShowStackTrace())
    return StackTrace();

  else
    return this->error_.c_str();
}

const char *HBaseException::StackTrace() const {

  return this->stack_trace_.c_str();
}

const bool &HBaseException::ShowStackTrace() const {

  return this->show_stack_trace_;
}

void HBaseException::SetStackTrace(const bool &show_stack_trace) {

  this->show_stack_trace_ = show_stack_trace;
}

const HBaseException::HBASEERRCODES &HBaseException::GetHBaseErrorCode() const {

  return this->err_code_;
}

const bool &HBaseException::RetryException() const {

  return this->retry_;
}


