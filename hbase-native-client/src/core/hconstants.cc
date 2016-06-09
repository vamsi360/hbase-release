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

#include "hconstants.h"

const int HBaseConstants::HConstants::DEFAULT_BLOCKSIZE = 64 * 1024;
/** Used as a magic return value while optimized index key feature enabled(HBASE-7845) */
const int HBaseConstants::HConstants::INDEX_KEY_MAGIC = -2;
/*
 * Name of directory that holds recovered edits written by the wal log
 * splitting code, one per region
 */
const std::string HBaseConstants::HConstants::RECOVERED_EDITS_DIR = "recovered.edits";
/**
 * The first four bytes of Hadoop RPC connections
 */
const char HBaseConstants::HConstants::RPC_HEADER[] = { 'H', 'B', 'a', 's', '\0'};
const char HBaseConstants::HConstants::RPC_CURRENT_VERSION = 0;
const int HBaseConstants::HConstants::MAX_ROW_LENGTH = std::numeric_limits< short >::max();
const long HBaseConstants::HConstants::NO_NONCE = 0;
const long HBaseConstants::HConstants::LATEST_TIMESTAMP = std::numeric_limits< long >::max();
/**
 * An empty instance.
 */
const BYTE_ARRAY HBaseConstants::HConstants::EMPTY_BYTE_ARRAY {};

const ByteBuffer HBaseConstants::HConstants::EMPTY_BYTE_BUFFER;// = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
const long HBaseConstants::HConstants::DEFAULT_MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024L;
const int HBaseConstants::HConstants::FOREVER = std::numeric_limits< int >::max();
const int HBaseConstants::HConstants::REPLICATION_SCOPE_LOCAL = 0;
const int HBaseConstants::HConstants::REPLICATION_SCOPE_GLOBAL = 1;

const char *HBaseConstants::HConstants::NAME = "NAME";
const char *HBaseConstants::HConstants::VERSIONS("VERSIONS");
const char *HBaseConstants::HConstants::IN_MEMORY = "IN_MEMORY";
const char *HBaseConstants::HConstants::METADATA = "METADATA";
const char *HBaseConstants::HConstants::CONFIGURATION = "CONFIGURATION";


/**
 * Configuration option that toggles whether EXEC permission checking is
 * performed during coprocessor endpoint invocations.
 */
const std::string HBaseConstants::AccessControlConstants::EXEC_PERMISSION_CHECKS_KEY = "hbase.security.exec.permission.checks";
/** Default setting for hbase.security.exec.permission.checks; false */
const bool HBaseConstants::AccessControlConstants::DEFAULT_EXEC_PERMISSION_CHECKS = false;

/**
 * Configuration or CF schema option for early termination of access checks
 * if table or CF permissions grant access. Pre-0.98 compatible behavior
 */
const std::string HBaseConstants::AccessControlConstants::CF_ATTRIBUTE_EARLY_OUT = "hbase.security.access.early_out";
/** Default setting for hbase.security.access.early_out */
const bool HBaseConstants::AccessControlConstants::DEFAULT_ATTRIBUTE_EARLY_OUT = true;

// Operation attributes for cell level security

/** Cell level ACL */
const std::string HBaseConstants::AccessControlConstants::OP_ATTRIBUTE_ACL = "acl";
/** Cell level ACL evaluation strategy */
const std::string HBaseConstants::AccessControlConstants::OP_ATTRIBUTE_ACL_STRATEGY = "acl.strategy";
/** Default cell ACL evaluation strategy: Table and CF first, then ACL */
const char * HBaseConstants::AccessControlConstants::OP_ATTRIBUTE_ACL_STRATEGY_DEFAULT = "0";
/** Alternate cell ACL evaluation strategy: Cell ACL first, then table and CF */
const char * HBaseConstants::AccessControlConstants::OP_ATTRIBUTE_ACL_STRATEGY_CELL_FIRST = "1";

const std::string HBaseConstants::EnumToString::ToString(const HBaseConstants::BLOOM_TYPE &enum_type) {
  std::string enum_string;
  switch (enum_type) {
  case HBaseConstants::BLOOM_TYPE::NONE:
    enum_string = "NONE";
    break;
  case HBaseConstants::BLOOM_TYPE::ROW:
    enum_string = "ROW";
    break;
  case HBaseConstants::BLOOM_TYPE::ROWCOL:
    enum_string = "ROWCOL";
    break;
  default:
    enum_string = "NONE";
    break;
  }
  return enum_string;
}

const std::string HBaseConstants::EnumToString::ToString(const HBaseConstants::DATA_BLOCK_ENCODING &enum_type) {
  std::string enum_string;
  switch (enum_type) {
  case HBaseConstants::DATA_BLOCK_ENCODING::DIFF:
    enum_string = "DIFF";
    break;
  case HBaseConstants::DATA_BLOCK_ENCODING::FAST_DIFF:
    enum_string = "FAST_DIFF";
    break;
  case HBaseConstants::DATA_BLOCK_ENCODING::NONE:
    enum_string = "NONE";
    break;
  case HBaseConstants::DATA_BLOCK_ENCODING::PREFIX:
    enum_string = "PREFIX";
    break;
  case HBaseConstants::DATA_BLOCK_ENCODING::PREFIX_TREE:
    enum_string = "PREFIX_TREE";
    break;
  default:
    enum_string = "NONE";
    break;
  }
  return enum_string;
}

const std::string HBaseConstants::EnumToString::ToString(const HBaseConstants::COMPRESSION_ALGORITHM &enum_type) {

  std::string enum_string;
  switch (enum_type) {
  case HBaseConstants::COMPRESSION_ALGORITHM::GZ:
    enum_string = "GZ";
    break;
  case HBaseConstants::COMPRESSION_ALGORITHM::LZ4:
    enum_string = "LZ4";
    break;
  case HBaseConstants::COMPRESSION_ALGORITHM::LZO:
    enum_string = "LZO";
    break;
  case HBaseConstants::COMPRESSION_ALGORITHM::NONE:
    enum_string = "NONE";
    break;
  case HBaseConstants::COMPRESSION_ALGORITHM::SNAPPY:
    enum_string = "SNAPPY";
    break;
  default:
    enum_string = "NONE";
    break;
  }
  return enum_string;
}

const std::string HBaseConstants::EnumToString::ToString(const HBaseConstants::KEEP_DELETED_CELLS &enum_type) {

  std::string enum_string;
  switch (enum_type) {
  case HBaseConstants::KEEP_DELETED_CELLS::FALSE:
    enum_string = "FALSE";
    break;
  case HBaseConstants::KEEP_DELETED_CELLS::TRUE:
    enum_string = "TRUE";
    break;
  case HBaseConstants::KEEP_DELETED_CELLS::TTL:
    enum_string = "TTL";
    break;
  }
  return enum_string;
}

const std::string HBaseConstants::EnumToString::ToString(const HBaseConstants::DURABILITY_TYPE &enum_type) {

  std::string enum_string;
  switch (enum_type){
  case HBaseConstants::DURABILITY_TYPE::ASYNC_WAL:
    enum_string = "ASYNC_WAL";
    break;
  case HBaseConstants::DURABILITY_TYPE::FSYNC_WAL:
    enum_string = "FSYNC_WAL";
    break;
  case HBaseConstants::DURABILITY_TYPE::SKIP_WAL:
    enum_string = "SKIP_WAL";
    break;
  case HBaseConstants::DURABILITY_TYPE::SYNC_WAL:
    enum_string = "SYNC_WAL";
    break;
  case HBaseConstants::DURABILITY_TYPE::USE_DEFAULT:
    enum_string = "USE_DEFAULT";
    break;
  }
  return enum_string;
}
