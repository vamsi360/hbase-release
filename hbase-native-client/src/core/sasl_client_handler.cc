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

#include "sasl_client_handler.h"

#include <memory>
#include <sasl/sasl.h>
#include <sasl/saslplug.h>
#include <sasl/saslutil.h>
#include <Poco/Timespan.h>
#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <sys/types.h>
#include <pwd.h>
#include "utils.h"


SaslClientHandler::SaslClientHandler(std::string &user,
    Poco::Net::StreamSocket *socket, const std::string &host_name,
    int port) :
    socket_(socket) {
  user_ = user;
  host_name_ = host_name;
  port_ = port;
  sconn_ = NULL;
  qop_enabled_ = false;
}

SaslClientHandler::~SaslClientHandler() {
  if (nullptr != sconn_) {
    sasl_dispose(&sconn_);
  }
  sconn_ = nullptr;
}

bool SaslClientHandler::Authenticate() {
  return Client();
}

bool SaslClientHandler::Client() {
  int rc;

  char *service_name = "hbase";

  /* Create new connection session. */
  rc = sasl_client_new(service_name, /* The service we are using*/
      host_name_.c_str(),
      NULL, NULL, /* Local and remote IP address strings
                        (NULL disables mechanisms which require this info)*/
      NULL, /*connection-specific callbacks*/
      0 /*security flags*/, &sconn_);

  if (rc != SASL_OK) {
    LOG(FATAL) << "Cannot initialize client ("<< rc << ") ";
    return false;
  }

  sasl_security_properties_t *props = new sasl_security_properties_t();
  ::memset(props, 0, sizeof(sasl_security_properties_t));
  props->max_ssf = 2;
  sasl_setprop(sconn_, SASL_SEC_PROPS, (void *)props);

  bool code = ClientAuthenticate();

  return code;
}

bool SaslClientHandler::ClientAuthenticate() {

  int rc;
  const char *out;
  unsigned int outlen;
  sasl_interact_t *client_interact=NULL;
  const char *in;
  int inlen;

  unsigned int uiTotalSize;
  const char *mechusing, *mechlist = "GSSAPI";

  do {
    rc = sasl_client_start(sconn_, /* the same context from above */
        mechlist, /* the list of mechanisms
		 from the server */
        NULL, /* filled in if an
		 interaction is needed */
        &out, /* filled in on success */
        &outlen, /* filled in on success */
        &mechusing);
  } while (rc == SASL_INTERACT); /* the mechanism may ask us to fill
	in things many times. result is SASL_CONTINUE on success */

  if (rc != SASL_CONTINUE) {
    LOG(FATAL)<< "Cannot start client ("<< rc << ") ";
    return false;
  }

  int buffer_size = 8 * 1024;
  std::vector<char> buffer(buffer_size + 1);
  std::fill(buffer.begin(), buffer.end(), 0);
  char *pBuf = &buffer[0];

  int bufferSize;
  int bytes_sent = 0;
  int bytes_received = 0;

  bool final_step_failed = false;

  do {
    bufferSize = outlen + 4;
    google::protobuf::uint8 *packet = new google::protobuf::uint8[bufferSize];
    ::memset(packet, '\0', bufferSize);

    google::protobuf::io::ArrayOutputStream aos(packet, bufferSize);
    google::protobuf::io::CodedOutputStream *coded_output = new google::protobuf::io::CodedOutputStream(&aos);

    uiTotalSize = outlen;
    CommonUtils::SwapByteOrder(uiTotalSize);
    coded_output->WriteRaw(&uiTotalSize, 4);
    coded_output->WriteRaw(out, outlen);

    bytes_sent = socket_->sendBytes(packet, bufferSize);

    if (rc == SASL_OK) {
      Poco::Timespan ts(4, 0);
      socket_->setReceiveTimeout(ts);
      try {
        bytes_received = 0;
        bytes_received = socket_->receiveBytes(pBuf, buffer_size);
      } catch (Poco::TimeoutException & toe) {
        //Safe To consume this exception
        //The last step of hand shake is successful as the
        //server didn't send any bytes back. As it's a blocking
        //socket, we want to timeout after a few seconds. If there
        //is any exception in the handshake the server would
        //send some bytes(like an exception or something)
      }
      Poco::Timespan ts1(0, 0);
      socket_->setReceiveTimeout(ts1);
      if (bytes_received > 0) {
        LOG(ERROR) << "Error in the final step of handshake";
        final_step_failed = true;
      }

      if (!final_step_failed) {
        const int *ssfp = nullptr;

        rc = sasl_getprop(sconn_, SASL_SSF, (const void**) &ssfp);
        if (rc != SASL_OK) {
          const char *err_msg = nullptr;
          sasl_errstring(rc, NULL, &err_msg);

          LOG(ERROR) << err_msg;
        }
        if (*ssfp > 0) {
          qop_enabled_ = true;
        }

        return true;
      }
    } else {
      bytes_received = socket_->receiveBytes(pBuf, buffer_size);
      if (bytes_received == 0)
      {
        return false;
      }
    }

    unsigned int *uiReader = (unsigned int *)pBuf;
    unsigned int status = *uiReader;

    char *p = pBuf + 4;

    uiReader = (unsigned int *)p;
    uiTotalSize = *uiReader;
    CommonUtils::SwapByteOrder(uiTotalSize);
    p = p + 4;

    size_t bs = uiTotalSize;

    char *temp = new char[uiTotalSize];
    ::memcpy(temp, p, uiTotalSize);
    ::memcpy(pBuf, temp, uiTotalSize);
    bs = uiTotalSize;
    delete []temp;

    if (final_step_failed || status != 0 /*Status 0 is success*/) {
      //Assumption here is not more than 8 * 1024
      pBuf[bs] = '\0';
      LOG(ERROR) << "Exception from server: "
          << pBuf;
      return false;
    }

    out = NULL;
    outlen = 0;

    rc = sasl_client_step(sconn_,  /* our context */
        pBuf,    /* the data from the server */
        bs, /* it's length */
        NULL,  /* this should be unallocated and NULL */
        &out,     /* filled in on success */
        &outlen); /* filled in on success */

  } while (rc == SASL_OK || rc == SASL_CONTINUE);

  if (rc != SASL_OK) {
    return false;
  }

  return true;
}

bool SaslClientHandler::IsQopEnabled() {
  return qop_enabled_;
}

void SaslClientHandler::Wrap(const char *srcBytes, unsigned int srcBytesLen,
    const char **dstBytes, unsigned int *dstBytesLen) {
  if (!qop_enabled_) {
    char *message = "No QOP found. Cannot wrap data ";
    LOG(INFO) << message;
    throw message;
  }

  *dstBytes = nullptr;
  *dstBytesLen = 0;

  int rc = sasl_encode(sconn_, srcBytes, srcBytesLen, dstBytes, dstBytesLen);
  if (rc != SASL_OK) {
    char *message = "Error wrapping data!!!";
    LOG(FATAL) << message;
    throw message;
  }
}

void SaslClientHandler::Unwrap(const char *srcBytes, unsigned int srcBytesLen,
    const char **dstBytes, unsigned int *dstBytesLen) {
  if (!qop_enabled_) {
    char *message = "No QOP found. Cannot unwrap data ";
    LOG(INFO) << message;
    throw message;
  }

  int rc = sasl_decode(sconn_, srcBytes, srcBytesLen, dstBytes, dstBytesLen);
  if (rc != SASL_OK) {
    char *message = "Error unwrapping data!!!";
    LOG(FATAL) << message;
    throw message;
  }
}

