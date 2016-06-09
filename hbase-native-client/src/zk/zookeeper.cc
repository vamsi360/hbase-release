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

#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>
#include <zookeeper/proto.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <string>
#include <functional>
#include "../rpc/generated/ZooKeeper.pb.h"
#include "zookeeper.h"
#include "utils.h"

using namespace std;


char PB_MAGIC[4] = {'P', 'B', 'U', 'F'};

class ZookeeperImpl {
  public:
    ZookeeperImpl(string &serverlist):connected_(false),
    zh_(NULL),reconnect_(false) {
      serverList_ = serverlist;
    }

  public:

    static void StaticWatcherCallbackFunction(zhandle_t *zh, int type,
        int state, const char *path, void *watcherCtx) {
      ZookeeperImpl *pImpl = (ZookeeperImpl *)watcherCtx;
      pImpl->watcherCallbackFunction(zh, type, state, path, watcherCtx);
    }

    void Init() {
      clientid_t cid;
      cid.client_id = 0;

      zh_ = zookeeper_init(serverList_.c_str(),
          &ZookeeperImpl::StaticWatcherCallbackFunction,
          120000, &cid, (void*)this, 0);
      if (NULL == zh_) {
        //			throw new std::exception("zookeeper_init failed with the code :");
      }
    }

    void Cleanup() {
      if (NULL == zh_) {
        return;
      }

      zookeeper_close(zh_);
      zh_ = NULL;
    }

    void watcherCallbackFunction(zhandle_t *zh, int type,
        int state, const char *path,void *watcherCtx) {
      if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
          // Connected (initial or reconnect).
          connected_ = true;
          reconnect_ = false;
        } else if (state == ZOO_CONNECTING_STATE) {
          // The client library automatically reconnects, taking
          // into account failed servers in the connection string,
          // appropriately handling the "herd effect", etc.

          // TODO(benh): If this watcher gets reused then the next
          // connected event will be perceived as a reconnect, but it
          // should not.
          reconnect_ = true;
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
          // If this watcher gets reused then the next connected
          // event shouldn't be perceived as a reconnect.
          reconnect_ = false;
        } else {
          //			LOG(FATAL) << "Unhandled ZooKeeper state (" << state << ")"
          //					   << " for ZOO_SESSION_EVENT";
          //			  throw new std::exception("Unhandled Zookeeper state");
        }
      } else if (type == ZOO_CHILD_EVENT) {
        //		  process::dispatch(pid, &T::updated, sessionId, path);
      } else if (type == ZOO_CHANGED_EVENT) {
        //		  process::dispatch(pid, &T::updated, sessionId, path);
      } else if (type == ZOO_CREATED_EVENT) {
        //		  process::dispatch(pid, &T::created, sessionId, path);
      } else if (type == ZOO_DELETED_EVENT) {
        //		  process::dispatch(pid, &T::deleted, sessionId, path);
      } else {
        //		  LOG(FATAL) << "Unhandled ZooKeeper event (" << type << ")"
        //					 << " in state (" << state << ")";
        std::string message;
        //			message = "Unhandled ZooKeeper event (" + type + ")" +
        //										  " in state (" + state + ")";
        //			throw new std::exception(message);
      }
    }

  public:
    std::string serverList_;
    zhandle_t *zh_;
    bool connected_;
    bool reconnect_;
};

Zookeeper::Zookeeper(string &serverlist, bool securedCluster,
                    const std::string &zkZnodeParent) {
  zkImpl_ = new ZookeeperImpl(serverlist);
  securedCluster_ = securedCluster;
  zkZnodeParent_ = zkZnodeParent;
}

Zookeeper::~Zookeeper() {
  Disconnect();
}

void Zookeeper::Connect() {
  zkImpl_->Init();
}

void Zookeeper::Disconnect() {
  if (NULL == zkImpl_) {
    return;
  }

  zkImpl_->Cleanup();
  zkImpl_ = NULL;
}


void Zookeeper::LocateHBaseMeta(std::string &serverName, int &port) {

  std::string meta_rs_node_name(zkZnodeParent_ + "/meta-region-server");

  int bufferLength = 512 * 1024 * 1024;
  char *pBuffer = new char[bufferLength];

  int status = zoo_get(zkImpl_->zh_, meta_rs_node_name.c_str(), 0, pBuffer, &bufferLength, NULL);
  if (status != ZOK) {
    delete []pBuffer;
    return;
  }

  char *pCurrent = pBuffer;
  char *pData = pBuffer;
  if (*pCurrent != -1) {
    pData = pCurrent;
  } else {
    pData++;
  }

  unsigned int *pidLength = (unsigned int *)pData;
  unsigned int idLength = *pidLength;
  CommonUtils::SwapByteOrder(idLength);
  int dataLength = bufferLength - 4 - idLength;
  int dataOffset = 4 + idLength;

  int lengthOfPBMagic = sizeof(PB_MAGIC);
  pData += dataOffset + lengthOfPBMagic;

  int bufferSize = bufferLength - (dataOffset + lengthOfPBMagic);

  google::protobuf::io::ArrayInputStream arr(pData, bufferSize);
  google::protobuf::io::CodedInputStream input(&arr);

  hbase::pb::MetaRegionServer metaRegionServer;
  bool success = metaRegionServer.ParseFromCodedStream(&input);

  hbase::pb::ServerName sName = metaRegionServer.server();
  serverName = sName.host_name();
  port = sName.port();

  delete []pBuffer;
}

void Zookeeper::LocateHBaseMaster(std::string &masterServerName,
    int &port) {
  std::string master_server_node_name(zkZnodeParent_ + "/master");

  int bufferLength = 512 * 1024 * 1024;
  char *pBuffer = new char[bufferLength];

  int status = zoo_get(zkImpl_->zh_,
      master_server_node_name.c_str(), 0,
      pBuffer, &bufferLength, NULL);
  if (status != ZOK) {
    delete []pBuffer;
    return;
  }

  char *pCurrent = pBuffer;
  char *pData = pBuffer;
  if (*pCurrent != -1) {
    pData = pCurrent;
  } else {
    pData++;
  }

  unsigned int *pidLength = (unsigned int *)pData;
  unsigned int idLength = *pidLength;
  CommonUtils::SwapByteOrder(idLength);
  int dataLength = bufferLength - 4 - idLength;
  int dataOffset = 4 + idLength;

  int lengthOfPBMagic = sizeof(PB_MAGIC);
  pData += dataOffset + lengthOfPBMagic;

  int bufferSize = bufferLength - (dataOffset + lengthOfPBMagic);

  google::protobuf::io::ArrayInputStream arr(pData, bufferSize);
  google::protobuf::io::CodedInputStream input(&arr);

  hbase::pb::Master master;
  bool success = master.ParseFromCodedStream(&input);

  const hbase::pb::ServerName &sName = master.master();
  masterServerName = sName.host_name();
  port = sName.port();

  delete []pBuffer;
}

bool Zookeeper::IsConnected() {
  return zkImpl_->connected_;
}
