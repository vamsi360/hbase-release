/**
 *
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
 */

package org.apache.hadoop.hbase.client;


import java.io.InterruptedIOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.BoundedCompletionService;

@InterfaceAudience.Private
class MetaRpcCallableWithReplicas extends RpcRetryingCallerWithReadReplicas {
  ExecutorService pool;
  ClusterConnection cConnection;
  RegionLocations metaLocations;
  byte[] metaKey;
  int rpcTimeout;

  public MetaRpcCallableWithReplicas(ExecutorService pool, ClusterConnection cConnection,
      byte[] metaKey, int rpcTimeout, int retries, int timeBeforeReplicas) {
    super(RpcControllerFactory.instantiate(cConnection.getConfiguration()),
        TableName.META_TABLE_NAME, cConnection, null, pool, retries,
        rpcTimeout, timeBeforeReplicas);
    this.metaKey = metaKey;
    this.rpcTimeout = rpcTimeout;
  }

  @Override
  protected byte[] getRowKey() {
    return metaKey;
  }

  @Override
  protected void addCallsForReplica(ResultBoundedCompletionService cs,
      RegionLocations rl, int min, int max) {
    for (int id = min; id <= max; id++) {
      HRegionLocation hrl = rl.getRegionLocation(id);
      MetaServerCallable callOnReplica = new MetaServerCallable(id, hrl);
      // this would retry internally (RetryingRPC#call). But when the RPC succeeds with
      // one of the other replicas the retry would be interrupted
      cs.submit(callOnReplica, rpcTimeout);
    }
  }

  class MetaServerCallable extends ReplicaRegionServerCallable {
    public MetaServerCallable(int id, HRegionLocation location) {
      super(id, location);
    }
    @Override
    public Result call() throws Exception {
      if (Thread.interrupted()) {
        throw new InterruptedIOException();
      }

      return ProtobufUtil.getRowOrBefore(getStub(),
          location.getRegionInfo().getRegionName(), metaKey,
          HConstants.CATALOG_FAMILY);
    }
  }
}
