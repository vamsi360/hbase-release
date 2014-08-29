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

import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.doFsck;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HBaseFsckRepair;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.apache.hadoop.hbase.zookeeper.LoadBalancerTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the scenarios where replicas are enabled for the meta table
 */
@Category(MediumTests.class)
public class TestMetaWithReplicas {
  static final Log LOG = LogFactory.getLog(TestMetaWithReplicas.class);
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Before
  public void setup() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.META_REPLICAS_NUM, 3);
    TEST_UTIL.getConfiguration().setInt(
        StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 1000);
    TEST_UTIL.startMiniCluster(3);    
    // disable the balancer
    LoadBalancerTracker l = new LoadBalancerTracker(TEST_UTIL.getZooKeeperWatcher(),
        new Abortable() {
      boolean aborted = false;
      @Override
      public boolean isAborted() {
        // TODO Auto-generated method stub
        return aborted;
      }
      @Override
      public void abort(String why, Throwable e) {
        aborted = true;        
      }
    });
    l.setBalancerOn(false);
    for (int replicaId = 1; replicaId < 3; replicaId ++) {
      HRegionInfo h = RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO,
        replicaId);
      TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().waitForAssignment(h);
    }
    LOG.debug("All meta replicas assigned");
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMetaHTDReplicaCount() throws Exception {
    assertTrue(HTableDescriptor.META_TABLEDESC.getRegionReplication() == 3);
    assertTrue(TEST_UTIL.getHBaseAdmin().getTableDescriptor(TableName.META_TABLE_NAME)
        .getRegionReplication() == 3);
  }
  
  @Test
  public void testZookeeperNodesForReplicas() throws Exception {
    // Checks all the znodes exist when meta's replicas are enabled
    ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    Configuration conf = TEST_UTIL.getConfiguration();
    String baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    // check that the data in the znode is parseable (this would also mean the znode exists)
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ServerName.parseFrom(data);
    for (int i = 1; i < 3; i++) {
      String secZnode = ZKUtil.joinZNode(baseZNode,
          conf.get("zookeeper.znode.metaserver", "meta-region-server") + "-" + i);
      String str = zkw.getZNodeForReplica(i);
      assertTrue(str.equals(secZnode));
      // check that the data in the znode is parseable (this would also mean the znode exists)
      data = ZKUtil.getData(zkw, secZnode);
      ServerName.parseFrom(data);
    }
  }

  @Test
  public void testShutdownHandling() throws Exception {
    // This test creates a table, flushes the meta (with 3 replicas), kills the
    // server holding the primary meta replica. Then it does a put/get into/from
    // the test table. The put/get operations would use the replicas to locate the
    // location of the test table's region
    shutdownMetaAndDoValidations(TEST_UTIL);
  } 
  public static void shutdownMetaAndDoValidations(HBaseTestingUtility util) throws Exception {
    // This test creates a table, flushes the meta (with 3 replicas), kills the
    // server holding the primary meta replica. Then it does a put/get into/from
    // the test table. The put/get operations would use the replicas to locate the
    // location of the test table's region
    ZooKeeperWatcher zkw = util.getZooKeeperWatcher();
    Configuration conf = new Configuration(util.getConfiguration());
    conf.setBoolean("hbase.cache.meta.location.enabled", false);
    conf.setBoolean(HConstants.USE_META_REPLICAS, true);

    String baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ServerName primary = ServerName.parseFrom(data);

    byte[] TABLE = Bytes.toBytes("testShutdownHandling");
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("foo") };
    HTable htable = util.createTable(TABLE, FAMILIES, conf);

    util.flush(TableName.META_TABLE_NAME);
    Thread.sleep(5000);
    CatalogTracker ct = new CatalogTracker(conf);
    List<HRegionInfo> regions = MetaReader.getTableRegions(ct, TableName.valueOf(TABLE));
    HRegionLocation hrl = MetaReader.getRegionLocation(ct, regions.get(0));
    // Ensure that the primary server for test table is not the same one as the primary
    // of the meta region since we will be killing the srv holding the meta's primary...
    // We want to be able to write to the test table even when the meta is not present ..
    // If the servers are the same, then move the test table's region out of the server
    // to another random server
    if (hrl.getServerName().equals(primary)) {
      util.getHBaseAdmin().move(hrl.getRegionInfo().getEncodedNameAsBytes(), null);
      // wait for the move to complete
      do {
        Thread.sleep(10);
        hrl = MetaReader.getRegionLocation(ct, regions.get(0));
      } while (primary.equals(hrl.getServerName()));
      util.flush(TableName.META_TABLE_NAME);
      Thread.sleep(5000);
    }
    ServerName master = util.getHBaseClusterInterface().getClusterStatus().getMaster();
    // kill the master so that regionserver recovery is not triggered at all
    // for the meta server
    util.getHBaseClusterInterface().stopMaster(master);
    util.getHBaseClusterInterface().waitForMasterToStop(master, 60000);
    util.getHBaseClusterInterface().killRegionServer(primary);
    util.getHBaseClusterInterface().waitForRegionServerToStop(primary, 60000);
    // clear the region cache.. actually not needed since the region cache would be empty
    // now, but doesn't harm to clear
    htable.clearRegionCache();
    htable.close();
    htable = new HTable(conf, TABLE);
    byte[] row = "test".getBytes();
    Put put = new Put(row);
    put.add("foo".getBytes(), row, row);
    htable.put(put);
    htable.flushCommits();
    // Try to do a get of the row that was just put
    Get get = new Get(row);
    Result r = htable.get(get);
    assertTrue(Arrays.equals(r.getRow(), row));
    // now start back the killed servers and disable use of replicas. That would mean
    // calls go to the primary
    util.getHBaseClusterInterface().startMaster(master.getHostname());
    util.getHBaseClusterInterface().startRegionServer(primary.getHostname());
    util.getHBaseClusterInterface().waitForActiveAndReadyMaster();
    htable.clearRegionCache();
    htable.close();
    conf.setBoolean(HConstants.USE_META_REPLICAS, false);
    htable = new HTable(conf, TABLE);
    r = htable.get(get);
    assertTrue(Arrays.equals(r.getRow(), row));
  }

  @Test
  public void testHBaseFsckWithMetaReplicas() throws Exception {
    HBaseFsck hbck = HbckTestingUtil.doFsck(TEST_UTIL.getConfiguration(), false);
    HbckTestingUtil.assertNoErrors(hbck);
  }

  @Test
  public void testHBaseFsckWithFewerMetaReplicas() throws Exception {
    ClusterConnection c = (ClusterConnection)HConnectionManager.createConnection(
        TEST_UTIL.getConfiguration());
    RegionLocations rl = c.locateRegion(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW,
        false, false);
    HBaseFsckRepair.closeRegionSilentlyAndWait(TEST_UTIL.getHBaseAdmin(),
        rl.getRegionLocation(1).getServerName(), rl.getRegionLocation(1).getRegionInfo());
    // check that problem exists
    HBaseFsck hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{ERROR_CODE.UNKNOWN,ERROR_CODE.NO_META_REGION});
    // fix the problem
    hbck = doFsck(TEST_UTIL.getConfiguration(), true);
    // run hbck again to make sure we don't see any errors
    hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{});
  }

  @Test
  public void testHBaseFsckWithFewerMetaReplicaZnodes() throws Exception {
    ClusterConnection c = (ClusterConnection)HConnectionManager.createConnection(
        TEST_UTIL.getConfiguration());
    RegionLocations rl = c.locateRegion(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW,
        false, false);
    HBaseFsckRepair.closeRegionSilentlyAndWait(TEST_UTIL.getHBaseAdmin(),
        rl.getRegionLocation(2).getServerName(), rl.getRegionLocation(2).getRegionInfo());
    ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    ZKUtil.deleteNode(zkw, zkw.getZNodeForReplica(2));
    // check that problem exists
    HBaseFsck hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{ERROR_CODE.UNKNOWN,ERROR_CODE.NO_META_REGION});
    // fix the problem
    hbck = doFsck(TEST_UTIL.getConfiguration(), true);
    // run hbck again to make sure we don't see any errors
    hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{});
  }

  @Test
  public void testHBaseFsckWithExcessMetaReplicas() throws Exception {
    HBaseFsck hbck = new HBaseFsck(TEST_UTIL.getConfiguration());
    // Create a meta replica (this will be the 4th one) and assign it
    HRegionInfo h = RegionReplicaUtil.getRegionInfoForReplica(
        HRegionInfo.FIRST_META_REGIONINFO, 3);
    // create in-memory state otherwise master won't assign
    TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
             .getRegionStates().createRegionState(h);
    TEST_UTIL.getMiniHBaseCluster().getMaster().assignRegion(h);
    HBaseFsckRepair.waitUntilAssigned(TEST_UTIL.getHBaseAdmin(), h);
    // check that problem exists
    hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{ERROR_CODE.UNKNOWN, ERROR_CODE.SHOULD_NOT_BE_DEPLOYED});
    // fix the problem
    hbck = doFsck(TEST_UTIL.getConfiguration(), true);
    // run hbck again to make sure we don't see any errors
    hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{});
  }
}
