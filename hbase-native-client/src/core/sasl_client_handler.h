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

#include <sys/types.h>
#include <sasl/sasl.h>
#include <Poco/Net/StreamSocket.h>
#include <string>

struct sasl_connt_t;

class SaslClientHandler {

 public:
  SaslClientHandler(std::string &user, Poco::Net::StreamSocket *socket,
			const std::string &host_name, int port);
  virtual ~SaslClientHandler();
  bool Authenticate();
  bool IsQopEnabled();
  void Wrap(const char *srcBytes, unsigned int srcBytesLen,
			const char **dstBytes, unsigned int *dstBytesLen);
  void Unwrap(const char *srcBytes, unsigned int srcBytesLen,
			const char **dstBytes, unsigned int *dstBytesLen);

 private:
  bool Client ();
  bool ClientAuthenticate ();
  Poco::Net::StreamSocket *socket_;
  std::string host_name_;
  int port_;
  sasl_conn_t *sconn_;
  std::string user_;
  bool qop_enabled_;
};
