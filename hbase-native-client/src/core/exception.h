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
#include <exception>
#include <string>
#include <map>

class HBaseException : public std::exception {

 public:

  enum class HBASEERRCODES{
    ERR_REGION_SERVER_NOT_SERVING,
    ERR_REGION_MOVED,
    ERR_UNKNOWN
  };

  using EXCEPTIONLIST_MAP = std::map<std::string, HBaseException::HBASEERRCODES>;
  using EXCEPTIONLIST_PAIR = std::pair<std::string, HBaseException::HBASEERRCODES>;
  using RETRY_EXCEPTIONLIST_MAP = std::map<HBaseException::HBASEERRCODES, bool>;
  using RETRY_EXCEPTIONLIST_PAIR = std::pair<HBaseException::HBASEERRCODES, bool>;
  HBaseException(const std::string &error, bool stack_trace = false);
  virtual ~HBaseException();
  virtual const char *what () const throw();
  static RETRY_EXCEPTIONLIST_MAP GetRetryExceptionList();
  static EXCEPTIONLIST_MAP GetExceptionList();
  void SetStackTrace(const bool &show_stack_trace);
  const HBaseException::HBASEERRCODES &GetHBaseErrorCode() const;
  const bool &RetryException() const;



 private:

  static const RETRY_EXCEPTIONLIST_MAP RETRY_EXCEPTIONS;
  static const EXCEPTIONLIST_MAP EXCEPTIONS;
  const char *StackTrace () const;
  const bool &ShowStackTrace() const;

  std::string error_;
  std::string stack_trace_;
  bool show_stack_trace_;
  bool retry_;
  HBaseException::HBASEERRCODES err_code_;
  mutable int retry_count_;
};

