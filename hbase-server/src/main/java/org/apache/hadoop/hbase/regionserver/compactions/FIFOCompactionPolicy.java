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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.IncreasingToUpperBoundRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * 
 * FIFO compaction policy selects only files which have all cells expired. 
 * The column family MUST have non-default TTL. One of the use cases for this 
 * policy is when we need to store raw data which will be post-processed later 
 * and discarded completely after quite short period of time. Raw time-series vs. 
 * time-based roll up aggregates and compacted time-series. We collect raw time-series
 * and store them into CF with FIFO compaction policy, periodically we run task 
 * which creates roll up aggregates and compacts time-series, the original raw data 
 * can be discarded after that.
 * 
 */
public class FIFOCompactionPolicy extends RatioBasedCompactionPolicy {
  
  private static final Log LOG = LogFactory.getLog(FIFOCompactionPolicy.class);


  public FIFOCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
    verifyConfig(conf);
  }

  private void verifyConfig(Configuration conf) {
    // Major compaction disabled (recommended)
    StringBuffer warn = new StringBuffer();
    if(conf.getLong(HConstants.MAJOR_COMPACTION_PERIOD, Long.MAX_VALUE) > 0){
      warn.append(":periodic major compactions must be disabled:");
    }
    // Splits must be disabled (required) - throw exception if default
    String splitPolicyClassName = conf.get(HConstants.HBASE_REGION_SPLIT_POLICY_KEY, 
      IncreasingToUpperBoundRegionSplitPolicy.class.getName());
    if(splitPolicyClassName.equals(IncreasingToUpperBoundRegionSplitPolicy.class.getName())){
      throw new RuntimeException("Default split policy for FIFO compaction"+
          " is not supported, aborting.");
    } else if( !splitPolicyClassName.equals(DisabledRegionSplitPolicy.class.getName())){
      warn.append(":region splits must be disabled:");
    }    
    // Maximum blocking # of files is high enough (1000+?)
    if(storeConfigInfo.getBlockingFileCount() < 1000){
      warn.append(":blocking # of store files is too low-"+
          storeConfigInfo.getBlockingFileCount()+":");
    }
    // MIN_VERSION = 0 (required) - throw exception if not 0
    if(conf.getInt(HColumnDescriptor.MIN_VERSIONS, 0) != 0){
      throw new RuntimeException("MIN_VERSIONS > 0 for FIFO compaction is not supported,"+
          " aborting.");
    }
    // TTL is not default - throw exception if default
    if(storeConfigInfo.getStoreFileTtl() == Long.MAX_VALUE)
    {
      throw new RuntimeException("Default TTL for FIFO compaction is not supported, aborting.");
    }    
  }


  @Override
  public CompactionRequest selectCompaction(Collection<StoreFile> candidateFiles,
      List<StoreFile> filesCompacting, boolean isUserCompaction, boolean mayUseOffPeak,
      boolean forceMajor) throws IOException {
    
    if(forceMajor){
      LOG.warn("Major compaction is not supported for FIFO compaction policy. Ignore the flag.");
    }
    // Nothing to compact
    Collection<StoreFile> toCompact = getExpiredStores(candidateFiles, filesCompacting);
    CompactionRequest result = new CompactionRequest(toCompact);
    return result;
  }

  @Override
  public boolean isMajorCompaction(Collection<StoreFile> filesToCompact) throws IOException {
    // No major compaction support
    return false;
  }

  @Override
  public boolean needsCompaction(Collection<StoreFile> storeFiles, 
      List<StoreFile> filesCompacting) {    
    return hasExpiredStores(storeFiles);
  }

  private  boolean hasExpiredStores(Collection<StoreFile> files) {
    long currentTime = 0;//EnvironmentEdgeManager.currentTime();
    for(StoreFile sf: files){
      // Check MIN_VERSIONS is in HStore removeUnneededFiles
      Long maxTs = sf.getReader().getMaxTimestamp();
      long maxTtl = storeConfigInfo.getStoreFileTtl();
      if(maxTs == null 
          || maxTtl == Long.MAX_VALUE
          || (currentTime - maxTtl < maxTs)){
        continue; 
      } else{
        return true;
      }
    }
    return false;
  }

  private  Collection<StoreFile> getExpiredStores(Collection<StoreFile> files,
    Collection<StoreFile> filesCompacting) {
    long currentTime = 0;//EnvironmentEdgeManager.currentTime();
    Collection<StoreFile> expiredStores = new ArrayList<StoreFile>();    
    for(StoreFile sf: files){
      // Check MIN_VERSIONS is in HStore removeUnneededFiles
      Long maxTs = sf.getReader().getMaxTimestamp();
      long maxTtl = storeConfigInfo.getStoreFileTtl();
      if(maxTs == null 
          || maxTtl == Long.MAX_VALUE
          || (currentTime - maxTtl < maxTs)){
        continue; 
      } else if(filesCompacting == null || filesCompacting.contains(sf) == false){
        expiredStores.add(sf);
      }
    }
    return expiredStores;
  }
}
