/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.rsgroup;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.google.common.net.HostAndPort;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({MediumTests.class})
public class TestRSGroups extends TestRSGroupsBase {
  protected static final Log LOG = LogFactory.getLog(TestRSGroups.class);
  private static HMaster master;
  private static boolean init = false;
  private static RSGroupAdminEndpoint RSGroupAdminEndpoint;


  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, RSGroupBasedLoadBalancer.class.getName());
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, RSGroupAdminEndpoint.class.getName());
    conf.setBoolean(HConstants.ZOOKEEPER_USEMULTI, true);
    final int numSlaves = conf.getInt(NUM_SLAVES_BASE_KEY, DEFAULT_NUM_SLAVES_BASE);
    TEST_UTIL.startMiniCluster(numSlaves);
    conf.set(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, Integer.toString(numSlaves));
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);

    admin = TEST_UTIL.getHBaseAdmin();
    cluster = TEST_UTIL.getHBaseCluster();
    master = ((MiniHBaseCluster)cluster).getMaster();

    //wait for balancer to come online
    final long waitTimeout = conf.getLong(WAIT_TIMEOUT_KEY, DEFAULT_WAIT_TIMEOUT);
    TEST_UTIL.waitFor(waitTimeout, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return master.isInitialized() &&
            ((RSGroupBasedLoadBalancer) master.getLoadBalancer()).isOnline();
      }
    });
    admin.setBalancerRunning(false,true);
    rsGroupAdmin = new VerifyingRSGroupAdminClient(rsGroupAdmin.newClient(TEST_UTIL.getConnection()),
        conf);
    RSGroupAdminEndpoint =
        master.getMasterCoprocessorHost().findCoprocessors(RSGroupAdminEndpoint.class).get(0);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void beforeMethod() throws Exception {
    if(!init) {
      init = true;
      afterMethod();
    }

  }

  @After
  public void afterMethod() throws Exception {
    deleteTableIfNecessary();
    deleteNamespaceIfNecessary();
    deleteGroups();

    final int numSlaves = conf.getInt(NUM_SLAVES_BASE_KEY, DEFAULT_NUM_SLAVES_BASE);
    int missing = numSlaves - getNumServers();
    LOG.info("Restoring servers: "+missing);
    for(int i=0; i<missing; i++) {
      ((MiniHBaseCluster)cluster).startRegionServer();
    }

    rsGroupAdmin.addRSGroup("master");
    ServerName masterServerName =
        ((MiniHBaseCluster)cluster).getMaster().getServerName();

    try {
      rsGroupAdmin.moveServers(
          Sets.newHashSet(masterServerName.getHostPort()),
          "master");
    } catch (Exception ex) {
      // ignore
    }
    final long waitTimeout = conf.getLong(WAIT_TIMEOUT_KEY, DEFAULT_WAIT_TIMEOUT);
    TEST_UTIL.waitFor(waitTimeout, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        LOG.info("Waiting for cleanup to finish " + rsGroupAdmin.listRSGroups());
        //Might be greater since moving servers back to default
        //is after starting a server

        return rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size()
            == numSlaves;
      }
    });
  }

  @Test
  public void testBasicStartUp() throws IOException {
    RSGroupInfo defaultInfo = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    assertEquals(4, defaultInfo.getServers().size());
    // Assignment of root and meta regions.
    int count = master.getAssignmentManager().getRegionStates().getRegionAssignments().size();
    //3 meta,namespace, group
    assertEquals(3, count);
  }

  @Test
  public void testNamespaceCreateAndAssign() throws Exception {
    LOG.info("testNamespaceCreateAndAssign");
    String nsName = tablePrefix+"_foo";
    final TableName tableName = TableName.valueOf(nsName, tablePrefix + "_testCreateAndAssign");
    RSGroupInfo appInfo = addGroup(rsGroupAdmin, "appInfo", 1);
    admin.createNamespace(NamespaceDescriptor.create(nsName)
        .addConfiguration(RSGroupInfo.NAMESPACEDESC_PROP_GROUP, "appInfo").build());
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    admin.createTable(desc);
    //wait for created table to be assigned
    final long waitTimeout = conf.getLong(WAIT_TIMEOUT_KEY, DEFAULT_WAIT_TIMEOUT);
    TEST_UTIL.waitFor(waitTimeout, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });
    ServerName targetServer =
        ServerName.parseServerName(appInfo.getServers().iterator().next().toString());
    AdminProtos.AdminService.BlockingInterface rs = admin.getConnection().getAdmin(targetServer);
    //verify it was assigned to the right group
    Assert.assertEquals(1, ProtobufUtil.getOnlineRegions(rs).size());
  }

  @Test
  public void testDefaultNamespaceCreateAndAssign() throws Exception {
    LOG.info("testDefaultNamespaceCreateAndAssign");
    final byte[] tableName = Bytes.toBytes(tablePrefix + "_testCreateAndAssign");
    admin.modifyNamespace(NamespaceDescriptor.create("default")
        .addConfiguration(RSGroupInfo.NAMESPACEDESC_PROP_GROUP, "default").build());
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    admin.createTable(desc);
    //wait for created table to be assigned
    final long waitTimeout = conf.getLong(WAIT_TIMEOUT_KEY, DEFAULT_WAIT_TIMEOUT);
    TEST_UTIL.waitFor(waitTimeout, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });
  }

  @Test
  public void testNamespaceConstraint() throws Exception {
    String nsName = tablePrefix+"_foo";
    String groupName = tablePrefix+"_foo";
    LOG.info("testNamespaceConstraint");
    rsGroupAdmin.addRSGroup(groupName);
    admin.createNamespace(NamespaceDescriptor.create(nsName)
        .addConfiguration(RSGroupInfo.NAMESPACEDESC_PROP_GROUP, groupName)
        .build());
    //test removing a referenced group
    try {
      rsGroupAdmin.removeRSGroup(groupName);
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
    //test modify group
    //changing with the same name is fine
    admin.modifyNamespace(
        NamespaceDescriptor.create(nsName)
          .addConfiguration(RSGroupInfo.NAMESPACEDESC_PROP_GROUP, groupName)
          .build());
    String anotherGroup = tablePrefix+"_anotherGroup";
    rsGroupAdmin.addRSGroup(anotherGroup);
    //test add non-existent group
    admin.deleteNamespace(nsName);
    rsGroupAdmin.removeRSGroup(groupName);
    try {
      admin.createNamespace(NamespaceDescriptor.create(nsName)
          .addConfiguration(RSGroupInfo.NAMESPACEDESC_PROP_GROUP, "foo")
          .build());
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
  }

  @Test
  public void testGroupInfoMultiAccessing() throws Exception {
    RSGroupInfoManager manager = RSGroupAdminEndpoint.getGroupInfoManager();
    final RSGroupInfo defaultGroup = manager.getRSGroup("default");
    // getRSGroup updates default group's server list
    // this process must not affect other threads iterating the list
    Iterator<HostAndPort> it = defaultGroup.getServers().iterator();
    manager.getRSGroup("default");
    it.next();
  }

  @Test
  public void testMisplacedRegions() throws Exception {
    final TableName tableName = TableName.valueOf(tablePrefix+"_testMisplacedRegions");
    LOG.info("testMisplacedRegions");

    final RSGroupInfo RSGroupInfo = addGroup(rsGroupAdmin, "testMisplacedRegions", 1);

    TEST_UTIL.createMultiRegionTable(tableName, new byte[]{'f'}, 15);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    RSGroupAdminEndpoint.getGroupInfoManager()
        .moveTables(Sets.newHashSet(tableName), RSGroupInfo.getName());

    assertTrue(rsGroupAdmin.balanceRSGroup(RSGroupInfo.getName()));

    TEST_UTIL.waitFor(60000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        ServerName serverName =
            ServerName.valueOf(RSGroupInfo.getServers().iterator().next().toString(), 1);
        return admin.getConnection().getAdmin()
            .getOnlineRegions(serverName).size() == 15;
      }
    });
  }

  @Test
  public void testCloneSnapshot() throws Exception {
    byte[] FAMILY = Bytes.toBytes("test");
    final TableName tableName = TableName.valueOf(tablePrefix + "_testCloneSnapshot");
    String snapshotName = tableName.getNameAsString() + "_snap";
    TableName clonedTableName = TableName.valueOf(tableName.getNameAsString() + "_clone");

    // create base table
    TEST_UTIL.createTable(tableName, FAMILY);

    // create snapshot
    admin.snapshot(snapshotName, tableName);

    // clone
    admin.cloneSnapshot(snapshotName, clonedTableName);
  }

}
