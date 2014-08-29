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
package org.apache.hadoop.hbase.zookeeper;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.zookeeper.KeeperException;

/**
 * Tracks the meta region server location node in zookeeper.
 * Meta region location is set by <code>RegionServerServices</code>.
 * This class has a watcher on the meta location and notices changes.
 */
@InterfaceAudience.Private
public class MetaRegionTracker extends ZooKeeperNodeTracker {
  final String metaServerZNode;
  final int replicaId;
  /**
   * Creates a meta region location tracker.
   *
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher
   * @param abortable
   * @param metaServerZNode
   * @param replicaId TODO
   */
  public MetaRegionTracker(ZooKeeperWatcher watcher, Abortable abortable, String metaServerZNode,
      int replicaId) {
    super(watcher, metaServerZNode, abortable);
    this.metaServerZNode = metaServerZNode;
    this.replicaId = replicaId;
  }

  /**
   * Checks if the meta region location is available.
   * @return true if meta region location is available, false if not
   */
  public boolean isLocationAvailable() {
    return super.getData(true) != null;
  }

  /**
   * Gets the meta region location, if available.  Does not block.  Sets a watcher.
   * @return server name or null if we failed to get the data.
   * @throws InterruptedException
   */
  public ServerName getMetaRegionLocation() throws InterruptedException {
    try {
      return ServerName.parseFrom(super.getData(true));
    } catch (DeserializationException e) {
      LOG.warn("Failed parse", e);
      return null;
    }
  }

  /**
   * Gets the meta region location, if available.  Does not block.  Does not set
   * a watcher (In this regard it differs from {@link #getMetaRegionLocation}.
   * @param zkw
   * @return server name or null if we failed to get the data.
   * @throws KeeperException
   */
  public static ServerName getMetaRegionLocation(final ZooKeeperWatcher zkw)
  throws KeeperException {
    return getMetaRegionLocation(zkw, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * Gets the meta region location, if available.  Does not block.  Does not set
   * a watcher (In this regard it differs from {@link #getMetaRegionLocation}.
   * @param zkw
   * @param replicaId
   * @return server name or null if we failed to get the data.
   * @throws KeeperException
   */
  public static ServerName getMetaRegionLocation(final ZooKeeperWatcher zkw,
      int replicaId) throws KeeperException {
    try {
      return ServerName.parseFrom(ZKUtil.getData(zkw, zkw.getZNodeForReplica(replicaId)));
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * Gets the meta region location, if available, and waits for up to the
   * specified timeout if not immediately available.
   * Given the zookeeper notification could be delayed, we will try to
   * get the latest data.
   * @param timeout maximum time to wait, in millis
   * @return server name for server hosting meta region formatted as per
   * {@link ServerName}, or null if none available
   * @throws InterruptedException if interrupted while waiting
   */
  public ServerName waitMetaRegionLocation(long timeout)
  throws InterruptedException {
    if (false == checkIfBaseNodeAvailable()) {
      String errorMsg = "Check the value configured in 'zookeeper.znode.parent'. "
          + "There could be a mismatch with the one configured in the master.";
      LOG.error(errorMsg);
      throw new IllegalArgumentException(errorMsg);
    }
    try {
      return ServerName.parseFrom(super.blockUntilAvailable(timeout, true));
    } catch (DeserializationException e) {
      LOG.warn("Failed parse", e);
      return null;
    }
  }

  /**
   * Sets the location of <code>hbase:meta</code> in ZooKeeper to the
   * specified server address.
   * @param zookeeper zookeeper reference
   * @param location The server hosting <code>hbase:meta</code>
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void setMetaLocation(ZooKeeperWatcher zookeeper,
                                     final ServerName location)
  throws KeeperException {
    setMetaLocation(zookeeper, HRegionInfo.DEFAULT_REPLICA_ID, location);
  }

  /**
   * Sets the location of <code>hbase:meta</code> in ZooKeeper to the
   * specified server address.
   * @param zookeeper zookeeper reference
   * @param replicaId
   * @param location The server hosting <code>hbase:meta</code>
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void setMetaLocation(ZooKeeperWatcher zookeeper, int replicaId,
      final ServerName location)
          throws KeeperException {
    LOG.info("Setting replica location for replicaId " + replicaId +
        " for hbase:meta region location in ZooKeeper as " + location);
    // Make the MetaRegionServer pb and then get its bytes and save this as
    // the znode content.
    byte [] data = toByteArray(location);
    try {
      ZKUtil.createAndWatch(zookeeper, zookeeper.getZNodeForReplica(replicaId), data);
    } catch(KeeperException.NodeExistsException nee) {
      LOG.debug("META region location already existed for " + replicaId + ", updated location");
      ZKUtil.setData(zookeeper, zookeeper.getZNodeForReplica(replicaId), data);
    }
  }

  /**
   * Build up the znode content.
   * @param sn What to put into the znode.
   * @return The content of the meta-region-server znode
   */
  static byte [] toByteArray(final ServerName sn) {
    // ZNode content is a pb message preceded by some pb magic.
    HBaseProtos.ServerName pbsn =
      HBaseProtos.ServerName.newBuilder()
                            .setHostName(sn.getHostname())
                            .setPort(sn.getPort())
                            .setStartCode(sn.getStartcode())
                            .build();

    ZooKeeperProtos.MetaRegionServer pbrsr =
      ZooKeeperProtos.MetaRegionServer.newBuilder()
                                      .setServer(pbsn)
                                      .setRpcVersion(HConstants.RPC_CURRENT_VERSION)
                                      .build();
    return ProtobufUtil.prependPBMagic(pbrsr.toByteArray());
  }

  /**
   * Deletes the location of <code>hbase:meta</code> in ZooKeeper.
   * @param zookeeper zookeeper reference
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void deleteMetaLocation(ZooKeeperWatcher zookeeper)
  throws KeeperException {
    deleteMetaLocation(zookeeper, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * Deletes the location of <code>hbase:meta</code> in ZooKeeper.
   * @param zookeeper zookeeper reference
   * @param replicaId
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void deleteMetaLocation(ZooKeeperWatcher zookeeper, int replicaId)
  throws KeeperException {
    LOG.info("Unsetting hbase:meta region location in ZooKeeper");
    try {
      // Just delete the node.  Don't need any watches.
      ZKUtil.deleteNode(zookeeper, zookeeper.getZNodeForReplica(replicaId));
    } catch(KeeperException.NoNodeException nne) {
      // Has already been deleted
    }
  }

  /**
   * Wait until the primary meta region is available. Get the secondary
   * locations as well but don't block for those.
   * @param zkw
   * @param timeout
   * @param conf
   * @return ServerName or null if we timed out.
   * @throws InterruptedException
   */
  public static List<ServerName> blockUntilAvailable(final ZooKeeperWatcher zkw,
      final long timeout, Configuration conf)
          throws InterruptedException {
    int numReplicasConfigured = 1;
    try {
      List<String> metaReplicaNodes = zkw.getMetaReplicaNodes();
      numReplicasConfigured = metaReplicaNodes.size();
    } catch (KeeperException e) {
      LOG.warn("Got ZK exception " + e);
    }
    List<ServerName> servers = new ArrayList<ServerName>(numReplicasConfigured);
    ServerName server = blockUntilAvailable(zkw, timeout);
    if (server == null) return null;
    servers.add(server);

    for (int replicaId = 1; replicaId < numReplicasConfigured; replicaId++) {
      byte [] data;
      // return all replica locations for the meta
      try {
        data = ZKUtil.getData(zkw, zkw.getZNodeForReplica(replicaId));
        try {
          servers.add(ServerName.parseFrom(data));
        } catch (DeserializationException d) {
          LOG.warn("Failed parse", d);
        }
      } catch (KeeperException e) {
        LOG.warn("Got ZK exception " + e);
      }   
    }
    // set the rest of the elements to null. Upper layer handles null locations for replicas.
    int serversSize = servers.size();
    if (serversSize != numReplicasConfigured) {
      for (int i = serversSize; i < numReplicasConfigured; i++) {
        servers.set(i, null);
      }
    }
    return servers;
  }

  /**
   * Wait until the primary meta region is available.
   * @param zkw
   * @param timeout
   * @return ServerName or null if we timed out.
   * @throws InterruptedException
   */
  public static ServerName blockUntilAvailable(final ZooKeeperWatcher zkw,
      final long timeout)
  throws InterruptedException {
    byte [] data = ZKUtil.blockUntilAvailable(zkw,
        zkw.getZNodeForReplica(HRegionInfo.DEFAULT_REPLICA_ID), timeout);
    if (data == null) return null;
    try {
      return ServerName.parseFrom(data);
    } catch (DeserializationException e) {
      LOG.warn("Failed parse", e);
      return null;
    }
  }
}
