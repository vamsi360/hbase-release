package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TestMetaWithReplicas;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * An integration test that starts the cluster with three replicas for the meta
 * It then creates a table, flushes the meta, kills the server holding the primary.
 * After that a client issues put/get requests on the created table - the other 
 * replicas of the meta would be used to get the location of the region of the created
 * table.
 */
@Category(IntegrationTests.class)
public class IntegrationTestMetaReplicas {
  static final Log LOG = LogFactory.getLog(IntegrationTestMetaReplicas.class);
  /**
   * Util to get at the cluster.
   */
  private static IntegrationTestingUtility util;

  @BeforeClass
  public static void setUp() throws Exception {
    // Set up the integration test util
    if (util == null) {
      util = new IntegrationTestingUtility();
    }
    util.getConfiguration().setInt(HConstants.META_REPLICAS_NUM, 3);
    util.getConfiguration().setInt(
        StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 1000);
    // Make sure there are three servers.
    util.initializeCluster(3);
    ZooKeeperWatcher zkw = util.getZooKeeperWatcher();
    Configuration conf = util.getConfiguration();
    String baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    // check that the data in the znode is parseable (this would also mean the znode exists)
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ServerName.parseFrom(data);
    waitUntilZnodeAvailable(1);
    waitUntilZnodeAvailable(2);
  }

  @AfterClass
  public static void teardown() throws Exception {
    //Clean everything up.
    util.restoreCluster();
    util = null;
  }

  private static void waitUntilZnodeAvailable(int replicaId) throws Exception {
    String znode = util.getZooKeeperWatcher().getZNodeForReplica(replicaId);
    int i = 0;
    while (i < 1000) {
      if (ZKUtil.checkExists(util.getZooKeeperWatcher(), znode) == -1) {
        Thread.sleep(100);
        i++;
      } else break;
    }
    if (i == 1000) throw new IOException("znode for meta replica " + replicaId + " not available");
  }

  @Test
  public void testShutdownHandling() throws Exception {
    // This test creates a table, flushes the meta (with 3 replicas), kills the
    // server holding the primary meta replica. Then it does a put/get into/from
    // the test table. The put/get operations would use the replicas to locate the
    // location of the test table's region
    TestMetaWithReplicas.shutdownMetaAndDoValidations(util);
  }
}
