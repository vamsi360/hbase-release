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

package org.apache.hadoop.hbase.backup.master;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupCopyService;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupPhase;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRestoreServerFactory;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.IncrementalBackupManager;
import org.apache.hadoop.hbase.backup.util.BackupClientUtil;
import org.apache.hadoop.hbase.backup.util.BackupServerUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.mapreduce.WALPlayer;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos.IncrementalTableBackupState;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos.ServerTimestamp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

@InterfaceAudience.Private
public class IncrementalTableBackupProcedure
    extends StateMachineProcedure<MasterProcedureEnv, IncrementalTableBackupState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(IncrementalTableBackupProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);
  private Configuration conf;
  private String backupId;
  private List<TableName> tableList;
  private String targetRootDir;
  HashMap<String, Long> newTimestamps = null;

  private BackupManager backupManager;
  private BackupInfo backupContext;

  public IncrementalTableBackupProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public IncrementalTableBackupProcedure(final MasterProcedureEnv env,
      final String backupId,
      List<TableName> tableList, String targetRootDir, final int workers,
      final long bandwidth) throws IOException {
    backupManager = new BackupManager(env.getMasterConfiguration());
    this.backupId = backupId;
    this.tableList = tableList;
    this.targetRootDir = targetRootDir;
    backupContext = backupManager.createBackupInfo(backupId, BackupType.INCREMENTAL, tableList,
          targetRootDir, workers, bandwidth);
  }

  @Override
  public byte[] getResult() {
    return backupId.getBytes();
  }

  private List<String> filterMissingFiles(List<String> incrBackupFileList) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    List<String> list = new ArrayList<String>();
    for(String file : incrBackupFileList){
      if(fs.exists(new Path(file))){
        list.add(file);
      } else{
        LOG.warn("Can't find file: "+file);
      }
    }
    return list;
  }

  Map<byte[], List<Path>>[] handleBulkLoad(List<TableName> sTableList) throws IOException {
    Map<byte[], List<Path>>[] mapForSrc = new Map[sTableList.size()];
    Pair<Map<TableName, Map<String, Map<String, List<Pair<String, Boolean>>>>>, List<byte[]>> pair =
        backupManager.readOrigBulkloadRows(sTableList);
    Map<TableName, Map<String, Map<String, List<Pair<String, Boolean>>>>> map = pair.getFirst();
    FileSystem fs = FileSystem.get(conf);
    FileSystem tgtFs;
    try {
      tgtFs = FileSystem.get(new URI(backupContext.getTargetRootDir()), conf);
    } catch (URISyntaxException use) {
      throw new IOException("Unable to get FileSystem", use);
    }
    Path rootdir = FSUtils.getRootDir(conf);
    Path tgtRoot = new Path(new Path(backupContext.getTargetRootDir()), backupId);
    LOG.debug("in handleBulkLoad, tgtRoot = " + tgtRoot);
    for (Map.Entry<TableName, Map<String, Map<String, List<Pair<String, Boolean>>>>> tblEntry :
      map.entrySet()) {
      TableName srcTable = tblEntry.getKey();
      int srcIdx = BackupSystemTable.getIndex(srcTable, sTableList);
      if (srcIdx < 0) {
        LOG.warn("Couldn't find " + srcTable + " in source table List");
        continue;
      }
      if (mapForSrc[srcIdx] == null) {
        mapForSrc[srcIdx] = new TreeMap<byte[], List<Path>>(Bytes.BYTES_COMPARATOR);
      }
      Path tblDir = FSUtils.getTableDir(rootdir, srcTable);
      Path tgtTable = new Path(new Path(tgtRoot, srcTable.getNamespaceAsString()),
          srcTable.getQualifierAsString());
      for (Map.Entry<String,Map<String,List<Pair<String, Boolean>>>> regionEntry :
        tblEntry.getValue().entrySet()){
        String regionName = regionEntry.getKey();
        Path regionDir = new Path(tblDir, regionName);
        // map from family to List of hfiles
        for (Map.Entry<String,List<Pair<String, Boolean>>> famEntry :
          regionEntry.getValue().entrySet()) {
          String fam = famEntry.getKey();
          Path famDir = new Path(regionDir, fam);
          List<Path> files;
          if (!mapForSrc[srcIdx].containsKey(fam.getBytes())) {
            files = new ArrayList<Path>();
            mapForSrc[srcIdx].put(fam.getBytes(), files);
          } else {
            files = mapForSrc[srcIdx].get(fam.getBytes());
          }
          Path archiveDir = HFileArchiveUtil.getStoreArchivePath(conf, srcTable, regionName, fam);
          String tblName = srcTable.getQualifierAsString();
          Path tgtFam = new Path(new Path(tgtTable, regionName), fam);
          if (!tgtFs.mkdirs(tgtFam)) {
            throw new IOException("couldn't create " + tgtFam);
          }
          for (Pair<String, Boolean> fileWithState : famEntry.getValue()) {
            String file = fileWithState.getFirst();
            boolean raw = fileWithState.getSecond();
            int idx = file.lastIndexOf("/");
            String filename = file;
            if (idx > 0) {
              filename = file.substring(idx+1);
            }
            Path p = new Path(famDir, filename);
            Path tgt = new Path(tgtFam, filename);
            Path archive = new Path(archiveDir, filename);
            LOG.debug("bulk testing " + p + " " + fs.exists(p));
            if (fs.exists(p)) {
              LOG.debug("found bulk hfile " + file + " in " + famDir + " for " + tblName);
              try {
                LOG.debug("copying " + p + " to " + tgt);
                FileUtil.copy(fs, p, tgtFs, tgt, false,conf);
              } catch (FileNotFoundException e) {
                LOG.debug("copying archive " + archive + " to " + tgt);
                try {
                  FileUtil.copy(fs, archive, tgtFs, tgt, false, conf);
                } catch (FileNotFoundException fnfe) {
                  if (!raw) throw fnfe;
                }
              }
            } else {
              LOG.debug("copying archive " + archive + " to " + tgt);
              try {
                FileUtil.copy(fs, archive, tgtFs, tgt, false, conf);
              } catch (FileNotFoundException fnfe) {
                if (!raw) throw fnfe;
              }
            }
            files.add(tgt);
          }
        }
      }
    }
    backupManager.writeBulkLoadedFiles(sTableList, mapForSrc);
    backupManager.removeOrigBulkLoadedRows(sTableList, pair.getSecond());
    return mapForSrc;
  }

  /**
   * Do incremental copy.
   * @param backupContext backup context
   */
  private void incrementalCopy(BackupInfo backupContext) throws Exception {

    LOG.info("Incremental copy is starting.");

    // set overall backup phase: incremental_copy
    backupContext.setPhase(BackupPhase.INCREMENTAL_COPY);

    // get incremental backup file list and prepare parms for DistCp
    List<String> incrBackupFileList = backupContext.getIncrBackupFileList();
    // filter missing files out (they have been copied by previous backups)
    incrBackupFileList = filterMissingFiles(incrBackupFileList);
    String[] strArr = incrBackupFileList.toArray(new String[incrBackupFileList.size() + 1]);
    strArr[strArr.length - 1] = backupContext.getHLogTargetDir();

    BackupCopyService copyService = BackupRestoreServerFactory.getBackupCopyService(conf);
    int res = copyService.copy(backupContext, backupManager, conf,
      BackupCopyService.Type.INCREMENTAL, strArr);

    if (res != 0) {
      LOG.error("Copy incremental log files failed with return code: " + res + ".");
      throw new IOException("Failed of Hadoop Distributed Copy from " + incrBackupFileList + " to "
          + backupContext.getHLogTargetDir());
    }
    LOG.info("Incremental copy from " + incrBackupFileList + " to "
        + backupContext.getHLogTargetDir() + " finished.");
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env,
      final IncrementalTableBackupState state) {
    if (conf == null) {
      conf = env.getMasterConfiguration();
    }
    if (backupManager == null) {
      try {
        backupManager = new BackupManager(env.getMasterConfiguration());
      } catch (IOException ioe) {
        setFailure("incremental backup", ioe);
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    try {
      switch (state) {
        case PREPARE_INCREMENTAL:
          FullTableBackupProcedure.beginBackup(backupManager, backupContext);
          LOG.debug("For incremental backup, current table set is "
              + backupManager.getIncrementalBackupTableSet());
          try {
            IncrementalBackupManager incrBackupManager =new IncrementalBackupManager(backupManager);

            newTimestamps = incrBackupManager.getIncrBackupLogFileList(backupContext);
          } catch (Exception e) {
            setFailure("Failure in incremental-backup: preparation phase " + backupId, e);
            // fail the overall backup and return
            FullTableBackupProcedure.failBackup(env, backupContext, backupManager, e,
              "Unexpected Exception : ", BackupType.INCREMENTAL, conf);
          }

          setNextState(IncrementalTableBackupState.INCREMENTAL_COPY);
          break;
        case INCREMENTAL_COPY:
          try {
            // copy out the table and region info files for each table
            BackupServerUtil.copyTableRegionInfo(backupContext, conf);
            // convert WAL to HFiles and copy them to .tmp under BACKUP_ROOT
            convertWALsAndCopy(backupContext, env.getMasterServices().getConnection());
            incrementalCopyHFiles(backupContext);
            // Save list of WAL files copied
            backupManager.recordWALFiles(backupContext.getIncrBackupFileList());
          } catch (Exception e) {
            String msg = "Unexpected exception in incremental-backup: incremental copy " + backupId;
            setFailure(msg, e);
            // fail the overall backup and return
            FullTableBackupProcedure.failBackup(env, backupContext, backupManager, e,
              msg, BackupType.INCREMENTAL, conf);
          }
          setNextState(IncrementalTableBackupState.INCR_BACKUP_COMPLETE);
          break;
        case INCR_BACKUP_COMPLETE:
          // set overall backup status: complete. Here we make sure to complete the backup.
          // After this checkpoint, even if entering cancel process, will let the backup finished
          backupContext.setState(BackupState.COMPLETE);
          // Set the previousTimestampMap which is before this current log roll to the manifest.
          HashMap<TableName, HashMap<String, Long>> previousTimestampMap =
              backupManager.readLogTimestampMap();
          backupContext.setIncrTimestampMap(previousTimestampMap);

          // The table list in backupContext is good for both full backup and incremental backup.
          // For incremental backup, it contains the incremental backup table set.
          backupManager.writeRegionServerLogTimestamp(backupContext.getTables(), newTimestamps);

          HashMap<TableName, HashMap<String, Long>> newTableSetTimestampMap =
              backupManager.readLogTimestampMap();

          Long newStartCode = BackupClientUtil
              .getMinValue(BackupServerUtil.getRSLogTimestampMins(newTableSetTimestampMap));
          backupManager.writeBackupStartCode(newStartCode);

          handleBulkLoad(backupContext.getTableNames());
          // backup complete
          FullTableBackupProcedure.completeBackup(env, backupContext, backupManager,
            BackupType.INCREMENTAL, conf);
          return Flow.NO_MORE_STATE;

        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      setFailure("snapshot-table", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env,
      final IncrementalTableBackupState state) throws IOException {
    // clean up the uncompleted data at target directory if the ongoing backup has already entered
    // the copy phase
    // For incremental backup, DistCp logs will be cleaned with the targetDir.
    FullTableBackupProcedure.cleanupTargetDir(backupContext, conf);
  }

  @Override
  protected IncrementalTableBackupState getState(final int stateId) {
    return IncrementalTableBackupState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final IncrementalTableBackupState state) {
    return state.getNumber();
  }

  @Override
  protected IncrementalTableBackupState getInitialState() {
    return IncrementalTableBackupState.PREPARE_INCREMENTAL;
  }

  @Override
  protected void setNextState(final IncrementalTableBackupState state) {
    if (aborted.get()) {
      setAbortFailure("snapshot-table", "abort requested");
    } else {
      super.setNextState(state);
    }
  }

  @Override
  public boolean abort(final MasterProcedureEnv env) {
    aborted.set(true);
    return true;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (targetRootDir=");
    sb.append(targetRootDir);
    sb.append("; backupId=").append(backupId);
    sb.append("; tables=");
    int len = tableList.size();
    for (int i = 0; i < len-1; i++) {
      sb.append(tableList.get(i)).append(",");
    }
    if (len >= 1) sb.append(tableList.get(len-1));
    sb.append(")");
  }

  BackupProtos.BackupProcContext toBackupInfo() {
    BackupProtos.BackupProcContext.Builder ctxBuilder = BackupProtos.BackupProcContext.newBuilder();
    ctxBuilder.setCtx(backupContext.toProtosBackupInfo());
    if (newTimestamps != null && !newTimestamps.isEmpty()) {
      BackupProtos.ServerTimestamp.Builder tsBuilder = ServerTimestamp.newBuilder();
      for (Entry<String, Long> entry : newTimestamps.entrySet()) {
        tsBuilder.clear().setServer(entry.getKey()).setTimestamp(entry.getValue());
        ctxBuilder.addServerTimestamp(tsBuilder.build());
      }
    }
    return ctxBuilder.build();
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    BackupProtos.BackupProcContext backupProcCtx = toBackupInfo();
    backupProcCtx.writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    BackupProtos.BackupProcContext proto =BackupProtos.BackupProcContext.parseDelimitedFrom(stream);
    backupContext = BackupInfo.fromProto(proto.getCtx());
    backupId = backupContext.getBackupId();
    targetRootDir = backupContext.getTargetRootDir();
    tableList = backupContext.getTableNames();
    List<ServerTimestamp> svrTimestamps = proto.getServerTimestampList();
    if (svrTimestamps != null && !svrTimestamps.isEmpty()) {
      newTimestamps = new HashMap<>();
      for (ServerTimestamp ts : svrTimestamps) {
        newTimestamps.put(ts.getServer(), ts.getTimestamp());
      }
    }
  }

  @Override
  public TableName getTableName() {
    return TableName.BACKUP_TABLE_NAME;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.BACKUP;
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    if (!env.isInitialized() && !getTableName().isSystemTable()) {
      return false;
    }
    return env.getProcedureQueue().tryAcquireTableWrite(getTableName(), "incremental backup");
    /*
    if (env.waitInitialized(this)) {
      return false;
    }
    return env.getProcedureQueue().tryAcquireTableExclusiveLock(this, TableName.BACKUP_TABLE_NAME);
    */
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseTableWrite(getTableName());
  }

   private void incrementalCopyHFiles(BackupInfo backupContext) throws Exception {

    LOG.info("Incremental copy HFiles is starting.");
    // set overall backup phase: incremental_copy
    backupContext.setPhase(BackupPhase.INCREMENTAL_COPY);
    // get incremental backup file list and prepare parms for DistCp
    List<String> incrBackupFileList = new ArrayList<String>();
    // Add Bulk output
    incrBackupFileList.add(getBulkOutputDir().toString());
    // filter missing files out (they have been copied by previous backups)
    String[] strArr = incrBackupFileList.toArray(new String[incrBackupFileList.size() + 1]);
    strArr[strArr.length - 1] = backupContext.getTargetRootDir();

    BackupCopyService copyService = BackupRestoreServerFactory.getBackupCopyService(conf);

    int res = copyService.copy(backupContext, backupManager, conf, BackupCopyService.Type.INCREMENTAL, strArr);

    if (res != 0) {
      LOG.error("Copy incremental HFile files failed with return code: " + res + ".");
      throw new IOException("Failed of Hadoop Distributed Copy from "
          + StringUtils.join(",", incrBackupFileList) + " to " + backupContext.getHLogTargetDir());
    }
    deleteBulkLoadDirectory();
    LOG.info("Incremental copy HFiles from " + StringUtils.join(",", incrBackupFileList) + " to "
        + backupContext.getTargetRootDir() + " finished.");
  }

  private void deleteBulkLoadDirectory() throws IOException {
    // delete original bulk load directory on method exit
    Path path = getBulkOutputDir();
    FileSystem fs = FileSystem.get(conf);
    boolean result = fs.delete(path, true);
    if (!result) {
      LOG.warn ("Could not delete " + path);
    }

}
  private void convertWALsAndCopy(BackupInfo backupContext, Connection conn) throws IOException {
    // get incremental backup file list and prepare parms for DistCp
    List<String> incrBackupFileList = backupContext.getIncrBackupFileList();
    // filter missing files out (they have been copied by previous backups)
    incrBackupFileList = filterMissingFiles(incrBackupFileList);
    // Get list of tables in incremental backup set
    Set<TableName> tableSet = backupManager.getIncrementalBackupTableSet();
    for(TableName table : tableSet) {
      // Check if table exists
      if(tableExists(table, conn)) {
        convertWALToHFiles(incrBackupFileList, table);
      } else {
        LOG.warn("Table "+ table+" does not exists. Skipping in WAL converter");
      }
    }

  }

  private boolean tableExists(TableName table, Connection conn) throws IOException {
    try (Admin admin = conn.getAdmin();) {
      return admin.tableExists(table);
    }
  }

  private void convertWALToHFiles(List<String> dirPaths, TableName tableName) throws IOException {

    String bulkOutputConfKey;
    Tool player = new WALPlayer();

    bulkOutputConfKey = WALPlayer.BULK_OUTPUT_CONF_KEY;

    // Player reads all files in arbitrary directory structure and creates
    // a Map task for each file. We use ';' as separator
    // because WAL file names contains ','
     String dirs = StringUtils.join(";", dirPaths);

     Path bulkOutputPath = getBulkOutputDirForTable(tableName);
     conf.set(bulkOutputConfKey, bulkOutputPath.toString());
     conf.set(WALPlayer.INPUT_FILES_SEPARATOR_KEY, ";");
     String[] playerArgs = { dirs, tableName.getNameAsString() };

     try {
       // TODO Player must tolerate missing files or exceptions during conversion
       player.setConf(conf);
       player.run(playerArgs);
      // TODO Check missing files and repeat
      conf.unset(WALPlayer.INPUT_FILES_SEPARATOR_KEY);
    } catch (Exception e) {
      throw new IOException("Can not convert from directory " + dirs
          + " (check Hadoop and HBase logs) ", e);
      }
    }

  private Path getBulkOutputDirForTable(TableName table) {
    Path tablePath = getBulkOutputDir();
    tablePath = new Path(tablePath, table.getNamespaceAsString());
    tablePath = new Path(tablePath, table.getQualifierAsString());
    return new Path(tablePath, "data");
  }

  private Path getBulkOutputDir() {
    String backupId = backupContext.getBackupId();
    Path path = new Path(backupContext.getTargetRootDir());
    path = new Path(path, ".tmp");
    path = new Path(path, backupId);
    return path;
  }


}
