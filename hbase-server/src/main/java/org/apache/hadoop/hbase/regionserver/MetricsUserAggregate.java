/**
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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import com.google.common.annotations.VisibleForTesting;

public class MetricsUserAggregate {

  /** Provider for mapping principal names to Users */
  private final UserProvider userProvider;

  private final MetricsUserAggregateSource source;

  public MetricsUserAggregate(Configuration conf) {
    source = CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
        .getUserAggregate();

    this.userProvider = UserProvider.instantiate(conf);
  }

  /**
   * Returns the active user to which authorization checks should be applied.
   * If we are in the context of an RPC call, the remote user is used,
   * otherwise the currently logged in user is used.
   */
  private String getActiveUser() {
    User user = RpcServer.getRequestUser();
    if (user == null) {
      // for non-rpc handling, fallback to system user
      try {
        user = userProvider.getCurrent();
      } catch (IOException ignore) {
      }
    }
    return user != null ? user.getShortName() : null;
  }

  @VisibleForTesting
  MetricsUserAggregateSource getSource() {
    return source;
  }

  public void updatePut(long t) {
    String user = getActiveUser();
    if (user != null) {
      source.getOrCreateMetricsUser(user).updatePut(t);
    }
  }

  public void updateDelete(long t) {
    String user = getActiveUser();
    if (user != null) {
      source.getOrCreateMetricsUser(user).updateDelete(t);
    }
  }

  public void updateGet(long t) {
    String user = getActiveUser();
    if (user != null) {
      source.getOrCreateMetricsUser(user).updateGet(t);
    }
  }

  public void updateIncrement(long t) {
    String user = getActiveUser();
    if (user != null) {
      source.getOrCreateMetricsUser(user).updateIncrement(t);
    }
  }

  public void updateAppend(long t) {
    String user = getActiveUser();
    if (user != null) {
      source.getOrCreateMetricsUser(user).updateAppend(t);
    }
  }

  public void updateReplay(long t) {
    String user = getActiveUser();
    if (user != null) {
      source.getOrCreateMetricsUser(user).updateReplay(t);
    }
  }

  public void updateScanTime(long t) {
    String user = getActiveUser();
    if (user != null) {
      source.getOrCreateMetricsUser(user).updateScanTime(t);
    }
  }
}
