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

package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;



@Category({MasterTests.class, LargeTests.class})
public class TestConcurrentModifyTableProc {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestConcurrentModifyTableProc.class);

  private HBaseTestingUtility UTIL;
  private MiniHBaseCluster cluster;
  private HMaster master;

  @Before public void setUp() throws Exception {
    UTIL = new HBaseTestingUtility();
    cluster = UTIL.startMiniCluster(1, 1);
    master = cluster.getMaster(0);
  }

  @Test public void testConcurrentModifyTable() throws IOException, InterruptedException {
    TableName table = TableName.valueOf("test");
    UTIL.createMultiRegionTable(table, Bytes.toBytes("C"), 10);
    Admin admin = UTIL.getAdmin();
    TableDescriptor tableDescriptor = admin.getDescriptor(table);
    ColumnFamilyDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();
    ProcedureExecutor<MasterProcedureEnv> executor = master.getMasterProcedureExecutor();

    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDescriptor);
    for (ColumnFamilyDescriptor descriptor : columnDescriptors) {
      ColumnFamilyDescriptorBuilder cfd = ColumnFamilyDescriptorBuilder.newBuilder(descriptor);
      cfd.setCompactionCompressionType(Compression.Algorithm.GZ);
      builder.modifyColumnFamily(cfd.build());
    }
    TableDescriptorBuilder builder2 = TableDescriptorBuilder.newBuilder(tableDescriptor);
    for (ColumnFamilyDescriptor descriptor : columnDescriptors) {
      ColumnFamilyDescriptorBuilder cfd = ColumnFamilyDescriptorBuilder.newBuilder(descriptor);
      cfd.setCompactionCompressionType(Compression.Algorithm.NONE);
      builder2.modifyColumnFamily(cfd.build());
    }

    HRegionServer rs = cluster.getRegionServer(0);
    Thread secondModifyTable = new Thread() {
      @Override public void run() {
        super.run();
        // Wait until first MoveRegionProcedure is executed
        UTIL.waitFor(30000,
          () -> executor.getProcedures().stream()
              .filter(p -> p instanceof MoveRegionProcedure)
              .map(p -> (MoveRegionProcedure) p).anyMatch(p -> table.equals(p.getTableName())));
        try {
          admin.modifyTable(builder2.build());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    secondModifyTable.start();
    admin.modifyTable(builder.build());
    // Wait for the second thread completed
    secondModifyTable.join();
    List<RegionInfo> regionList = admin.getRegions(table);
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    for (RegionInfo ri : regionList) {
      if (!regionStates.isRegionOnline(ri)) {
        fail("Region is not online: " + ri.getRegionNameAsString());
      }
    }
  }
}
