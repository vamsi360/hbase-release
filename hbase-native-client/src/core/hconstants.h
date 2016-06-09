/*
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
 *
 */

#pragma once

#include <iostream>
#include <limits>
#include <map>
#include <string>
#include <vector>


#include "bytes.h"

namespace HBaseConstants {
class HConstants {
 public:
  /**
   * Default block size for an HFile.
   */
  static const int DEFAULT_BLOCKSIZE;
  /** Used as a magic return value while optimized index key feature enabled(HBASE-7845) */
  static const int INDEX_KEY_MAGIC;
  /*
   * Name of directory that holds recovered edits written by the wal log
   * splitting code, one per region
   */
  static const std::string RECOVERED_EDITS_DIR;
  /**
   * The first four bytes of Hadoop RPC connections
   */
  static const char RPC_HEADER[];
  static const char RPC_CURRENT_VERSION;
  /**
   * Max length a row can have because of the limitation in TFile.
   */
  static const int MAX_ROW_LENGTH;
  static const long NO_NONCE;
  /**
   * Timestamp to use when we want to refer to the latest cell.
   * This is the timestamp sent by clients when no timestamp is specified on
   * commit.
   */
  static const long LATEST_TIMESTAMP;
  /**
   * An empty instance.
   */
  static const BYTE_ARRAY EMPTY_BYTE_ARRAY;

  static const ByteBuffer EMPTY_BYTE_BUFFER;

  /** Default maximum file size */
  static const long DEFAULT_MAX_FILE_SIZE;

  /**
   * Unlimited time-to-live.
   */
  static const int FOREVER;

  /**
   * Scope tag for locally scoped data.
   * This data will not be replicated.
   */
  static const int REPLICATION_SCOPE_LOCAL;

  /**
   * Scope tag for globally scoped data.
   * This data will be replicated to all peers.
   */
  static const int REPLICATION_SCOPE_GLOBAL;
  //TODO: although the following are referenced widely to format strings for
  //      the shell. They really aren't a part of the public API. It would be
  //      nice if we could put them somewhere where they did not need to be
  //      public. They could have package visibility
  static const char *NAME;
  static const char *VERSIONS;
  static const char *IN_MEMORY;
  static const char *METADATA;
  static const char *CONFIGURATION;

};


class AccessControlConstants {

 public:
  /**
   * Configuration option that toggles whether EXEC permission checking is
   * performed during coprocessor endpoint invocations.
   */
  static const std::string EXEC_PERMISSION_CHECKS_KEY;
  /** Default setting for hbase.security.exec.permission.checks; false */
  static const bool DEFAULT_EXEC_PERMISSION_CHECKS;

  /**
   * Configuration or CF schema option for early termination of access checks
   * if table or CF permissions grant access. Pre-0.98 compatible behavior
   */
  static const std::string CF_ATTRIBUTE_EARLY_OUT;
  /** Default setting for hbase.security.access.early_out */
  static const bool DEFAULT_ATTRIBUTE_EARLY_OUT;

  // Operation attributes for cell level security
  /** Cell level ACL */
  static const std::string OP_ATTRIBUTE_ACL;
  /** Cell level ACL evaluation strategy */
  static const std::string OP_ATTRIBUTE_ACL_STRATEGY;
  /** Default cell ACL evaluation strategy: Table and CF first, then ACL */
  static const char * OP_ATTRIBUTE_ACL_STRATEGY_DEFAULT;
  /** Alternate cell ACL evaluation strategy: Cell ACL first, then table and CF */
  static const char * OP_ATTRIBUTE_ACL_STRATEGY_CELL_FIRST;
};

/**
 * Ways to keep cells marked for delete around.
 */
/*
 * Don't change the TRUE/FALSE labels below, these have to be called
 * this way for backwards compatibility.
 */
enum class KEEP_DELETED_CELLS {
  /** Deleted Cells are not retained. */
  FALSE,
  /**
   * Deleted Cells are retained until they are removed by other means
   * such TTL or VERSIONS.
   * If no TTL is specified or no new versions of delete cells are
   * written, they are retained forever.
   */
  TRUE,
  /**
   * Deleted Cells are retained until the delete marker expires due to TTL.
   * This is useful when TTL is combined with MIN_VERSIONS and one
   * wants to keep a minimum number of versions around but at the same
   * time remove deleted cells after the TTL.
   */
  TTL
};

enum class BLOOM_TYPE {
  /**
   * Bloomfilters disabled
   */
  NONE,
  /**
   * Bloom enabled with Table row as Key
   */
  ROW,
  /**
   * Bloom enabled with Table row &amp; column (family+qualifier) as Key
   */
  ROWCOL
};

enum class DATA_BLOCK_ENCODING {

  /** Disable data block encoding. */
  NONE = 0,
      // id 1 is reserved for the BITSET algorithm to be added later
      PREFIX = 2,
      DIFF = 3,
      FAST_DIFF = 4,
      // COPY_KEY(5, "org.apache.hadoop.hbase.io.encoding.CopyKeyDataBlockEncoder"),
      PREFIX_TREE = 6
};

enum class COMPRESSION_ALGORITHM {
  LZO,
  GZ,
  NONE,
  SNAPPY,
  LZ4
};

enum class DURABILITY_TYPE {
  /* Developer note: Do not rename the enum field names. They are serialized in HTableDescriptor */
  /**
   * If this is for tables durability, use HBase's global default value (SYNC_WAL).
   * Otherwise, if this is for mutation, use the table's default setting to determine durability.
   * This must remain the first option.
   */
  USE_DEFAULT,
  /**
   * Do not write the Mutation to the WAL
   */
  SKIP_WAL,
  /**
   * Write the Mutation to the WAL asynchronously
   */
  ASYNC_WAL,
  /**
   * Write the Mutation to the WAL synchronously.
   * The data is flushed to the filesystem implementation, but not necessarily to disk.
   * For HDFS this will flush the data to the designated number of DataNodes.
   * See <a href="https://issues.apache.org/jira/browse/HADOOP-6313">HADOOP-6313</a>
   */
  SYNC_WAL,
  /**
   * Write the Mutation to the WAL synchronously and force the entries to disk.
   * (Note: this is currently not supported and will behave identical to {@link #SYNC_WAL})
   * See <a href="https://issues.apache.org/jira/browse/HADOOP-6313">HADOOP-6313</a>
   */
  FSYNC_WAL
};

class EnumToString {

 public:
  static const std::string ToString(const HBaseConstants::BLOOM_TYPE &enum_type);
  static const std::string ToString(const HBaseConstants::DATA_BLOCK_ENCODING &enum_type);
  static const std::string ToString(const HBaseConstants::COMPRESSION_ALGORITHM &enum_type);
  static const std::string ToString(const HBaseConstants::KEEP_DELETED_CELLS &enum_type);
  static const std::string ToString(const HBaseConstants::DURABILITY_TYPE &enum_type);
};


}
