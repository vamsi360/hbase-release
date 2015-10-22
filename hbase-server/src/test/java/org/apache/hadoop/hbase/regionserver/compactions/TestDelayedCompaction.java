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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class TestDelayedCompaction {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();


  private final TableName tableName = TableName.valueOf(getClass().getSimpleName());

  private final byte[] family = Bytes.toBytes("f");

  private final byte[] qualifier = Bytes.toBytes("q");

  private Store getStoreWithName(TableName tableName) {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (HRegion region : hrs.getOnlineRegions(tableName)) {
        return region.getStores().values().iterator().next();
      }
    }
    return null;
  }

  private Store prepareData(boolean delay) throws IOException, InterruptedException {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor colDesc = new HColumnDescriptor(family);
    // 10 sec delay
    if(delay){
      colDesc.setConfiguration(CompactionConfiguration.HBASE_HSTORE_COMPACTION_DELAY, "10"); 
    }
    desc.addFamily(colDesc);

    admin.createTable(desc);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Random rand = new Random();
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 10; j++) {
        byte[] value = new byte[128 * 1024];
        rand.nextBytes(value);
        table.put(new Put(Bytes.toBytes(i * 10 + j)).add(family, qualifier, value));
      }
      admin.flush(tableName.getName());
    }
    return getStoreWithName(tableName);
  }

  @Test
  public void testDelayedCompaction() throws Exception {

    TEST_UTIL.startMiniCluster(1);
    try {
      Store store = prepareData(true);
      assertEquals(3, store.getStorefilesCount());
      TEST_UTIL.getHBaseAdmin().compact(tableName.getName());
      Thread.sleep(2000);
      assertEquals(3, store.getStorefilesCount());
      Thread.sleep(8100);      
      TEST_UTIL.getHBaseAdmin().compact(tableName.getName());
      while (store.getStorefilesCount() > 1) {
        Thread.sleep(1000);
      }
      assertTrue(true);
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testCompactionNoDelay() throws Exception {

    TEST_UTIL.startMiniCluster(1);
    try {
      Store store = prepareData(false);
      assertEquals(3, store.getStorefilesCount());
      TEST_UTIL.getHBaseAdmin().compact(tableName.getName());
      Thread.sleep(2000);
      while (store.getStorefilesCount() > 1) {
        Thread.sleep(1000);
      }
      assertTrue(true);
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }
  
}
