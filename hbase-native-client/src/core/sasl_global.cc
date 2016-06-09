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

#include "sasl_global.h"

#include <memory>
#include <sasl/sasl.h>
#include <sasl/saslplug.h>
#include <sasl/saslutil.h>
#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include "utils.h"

static const char *progname = "HBase C++ Client";

int SaslGlobalData::SaslLogFn(void *context __attribute__((unused)),
    int priority,
    const char *message) {
  const char *label;

  if (! message)
    return SASL_BADPARAM;

  switch (priority) {
  case SASL_LOG_ERR:
    LOG(ERROR) << progname << ": SASL " << message;
    break;
  case SASL_LOG_NOTE:
    LOG(INFO) << progname << ": SASL " << message;
    break;
  default:
    LOG(INFO) << progname << ": SASL " << message;
    break;
  }

  return SASL_OK;
}

int SaslGlobalData::GetPluginPath(void *context,
    const char **path) {
  const char *searchpath = (const char *) context;

  if (!path)
    return SASL_BADPARAM;

  if (searchpath) {
    *path = searchpath;
  }

  return SASL_OK;
}

int SaslGlobalData::Simple(void *context,
    int id,
    const char **result,
    unsigned *len) {
  const char *value = (const char *)context;

  if (!result)
    return SASL_BADPARAM;

  switch (id) {
  case SASL_CB_USER: {
    *result = value;
    if (len)
      *len = value ? (unsigned) strlen(value) : 0;
  }
  break;
  default:
    return SASL_BADPARAM;
  }

  return SASL_OK;
}

SaslGlobalData::SaslGlobalData(std::string & user) {
  initialized_ = false;
  user_ = user;
  CALLBACK_COUNT_ = 4;
}

SaslGlobalData::~SaslGlobalData() {
  Cleanup();
}

bool SaslGlobalData::Init() {

  char *mech = "GSSAPI";
  char *searchpath = ::getenv("CYRUS_SASL_PLUGINS_DIR");

  if (NULL == searchpath ||
      0 == strcmp(searchpath, "")) {
    LOG(FATAL) << "Env. variable CYRUS_SASL_PLUGINS_DIR not found."
        << "Cannot load the GSSAPI plugin for kerberos authentication";
    return false;
  }

  std::string sasl_plugin_dir_path = searchpath;

  sasl_callback_t callbacks[CALLBACK_COUNT_];
  sasl_callback_t *callback;

  /* Fill in the callbacks that we're providing... */
  callback = callbacks;

  /* log */
  callback->id = SASL_CB_LOG;
  callback->proc = (sasl_callback_ft) &SaslLogFn;
  callback->context = NULL;
  ++callback;

  callback->id = SASL_CB_GETPATH;
  callback->proc = (sasl_callback_ft) &GetPluginPath;
  callback->context = (void *)(sasl_plugin_dir_path.c_str());
  ++callback;

  /* user */
  callback->id = SASL_CB_USER;
  callback->proc = (sasl_callback_ft) &Simple;
  callback->context = const_cast<char *>(user_.c_str());
  ++callback;

  /* termination */
  callback->id = SASL_CB_LIST_END;
  callback->proc = NULL;
  callback->context = NULL;
  ++callback;

  int rc;

  rc = sasl_client_init(callbacks);
  if (rc != SASL_OK) {
    LOG(FATAL)<< "SASL initialization failure ("
        << rc << ") ";
    return false;
  }

  initialized_ = true;

  return true;
}

void SaslGlobalData::Cleanup() {
  if (!initialized_) return;

  int rc = sasl_client_done();
  if (rc != SASL_OK) {
    LOG(INFO)<< "SASL cleanup failure ("
        << rc << ") ";
  }

  initialized_ = false;
}

