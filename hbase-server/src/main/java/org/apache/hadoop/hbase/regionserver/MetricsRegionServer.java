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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.TableName;

/**
 * This class is for maintaining the various regionserver statistics
 * and publishing them through the metrics interfaces.
 * <p/>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Private
public class MetricsRegionServer {
  public static final String RS_ENABLE_TABLE_METRICS_KEY =
      "hbase.regionserver.enable.table.latencies";
  public static final boolean RS_ENABLE_TABLE_METRICS_DEFAULT = true;

  private final MetricsRegionServerSource serverSource;
  private final MetricsRegionServerWrapper regionServerWrapper;
  private final MetricsTable metricsTable;
  private final RegionServerTableMetrics tableMetrics;

  private final MetricsUserAggregate userAggregate;

  public MetricsRegionServer(Configuration conf, MetricsRegionServerWrapper regionServerWrapper,
                             MetricsTable metricsTable) {
    this(conf, regionServerWrapper,
        CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
            .createServer(regionServerWrapper), metricsTable, createTableMetrics(conf));

  }

  MetricsRegionServer(Configuration conf, MetricsRegionServerWrapper regionServerWrapper,
                      MetricsRegionServerSource serverSource, MetricsTable metricsTable,
                      RegionServerTableMetrics tableMetrics) {
    this.regionServerWrapper = regionServerWrapper;
    this.serverSource = serverSource;
    this.userAggregate = new MetricsUserAggregate(conf);
    this.metricsTable = metricsTable;
    this.tableMetrics = tableMetrics;
  }

  /**
   * Creates an instance of {@link RegionServerTableMetrics} only if the feature is enabled.
   */
  static RegionServerTableMetrics createTableMetrics(Configuration conf) {
    if (conf.getBoolean(RS_ENABLE_TABLE_METRICS_KEY, RS_ENABLE_TABLE_METRICS_DEFAULT)) {
      return new RegionServerTableMetrics();
    }
    return null;
  }

  // for unit-test usage
  public MetricsRegionServerSource getMetricsSource() {
    return serverSource;
  }

  @VisibleForTesting
  public MetricsUserAggregate getMetricsUserAggregate() {
    return userAggregate;
  }

  public MetricsRegionServerWrapper getRegionServerWrapper() {
    return regionServerWrapper;
  }

  public void updatePut(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updatePut(tn, t);
    }
    if (t > 1000) {
      serverSource.incrSlowPut();
    }
    serverSource.updatePut(t);
    userAggregate.updatePut(t);
  }

  public void updateDelete(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateDelete(tn, t);
    }
    if (t > 1000) {
      serverSource.incrSlowDelete();
    }
    serverSource.updateDelete(t);
    userAggregate.updateDelete(t);
  }

  public void updateGet(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateGet(tn, t);
    }
    if (t > 1000) {
      serverSource.incrSlowGet();
    }
    serverSource.updateGet(t);
    userAggregate.updateGet(t);
  }

  public void updateIncrement(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateIncrement(tn, t);
    }
    if (t > 1000) {
      serverSource.incrSlowIncrement();
    }
    serverSource.updateIncrement(t);
    userAggregate.updateIncrement(t);
  }

  public void updateAppend(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateAppend(tn, t);
    }
    if (t > 1000) {
      serverSource.incrSlowAppend();
    }
    serverSource.updateAppend(t);
    userAggregate.updateAppend(t);
  }

  public void updateReplay(long t){
    serverSource.updateReplay(t);
    userAggregate.updateReplay(t);
  }

  public void updateScanSize(TableName tn, long scanSize){
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateScanSize(tn, scanSize);
    }
    serverSource.updateScanSize(scanSize);
  }

  public void updateScanTime(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateScanTime(tn, t);
    }
    serverSource.updateScanTime(t);
    userAggregate.updateScanTime(t);
  }

  public void updateSplitTime(long t) {
    serverSource.updateSplitTime(t);
  }

  public void incrSplitRequest() {
    serverSource.incrSplitRequest();
  }

  public void incrSplitSuccess() {
    serverSource.incrSplitSuccess();
  }

  public void updateFlush(String table, long t, long memstoreSize, long fileSize) {
    serverSource.updateFlushTime(t);
    serverSource.updateFlushMemstoreSize(memstoreSize);
    serverSource.updateFlushOutputSize(fileSize);

    if (table != null) {
      metricsTable.updateFlushTime(table, memstoreSize);
      metricsTable.updateFlushMemstoreSize(table, memstoreSize);
      metricsTable.updateFlushOutputSize(table, fileSize);
    }
  }

  public void updateCompaction(String table, boolean isMajor, long t, int inputFileCount,
      int outputFileCount, long inputBytes, long outputBytes) {
    serverSource.updateCompactionTime(isMajor, t);
    serverSource.updateCompactionInputFileCount(isMajor, inputFileCount);
    serverSource.updateCompactionOutputFileCount(isMajor, outputFileCount);
    serverSource.updateCompactionInputSize(isMajor, inputBytes);
    serverSource.updateCompactionOutputSize(isMajor, outputBytes);

    if (table != null) {
      metricsTable.updateCompactionTime(table, isMajor, t);
      metricsTable.updateCompactionInputFileCount(table, isMajor, inputFileCount);
      metricsTable.updateCompactionOutputFileCount(table, isMajor, outputFileCount);
      metricsTable.updateCompactionInputSize(table, isMajor, inputBytes);
      metricsTable.updateCompactionOutputSize(table, isMajor, outputBytes);
    }
  }
}
