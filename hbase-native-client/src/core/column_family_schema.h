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
#include <map>
#include <vector>

#include "bytes.h"
#include "hconstants.h"

class ColumnFamilySchema {
 private:
  // For future backward compatibility

  // Version  3 was when column names become byte arrays and when we picked up
  // Time-to-live feature.  Version 4 was when we moved to byte arrays, HBASE-82.
  // Version  5 was when bloom filter descriptors were removed.
  // Version  6 adds metadata as a map where keys and values are byte[].
  // Version  7 -- add new compression and hfile blocksize to HColumnDescriptor (HBASE-1217)
  // Version  8 -- reintroduction of bloom filters, changed from boolean to enum
  // Version  9 -- add data block encoding
  // Version 10 -- change metadata to standard type.
  // Version 11 -- add column family level configuration.
  static const BYTE_TYPE COLUMN_DESCRIPTOR_VERSION;

 public:


  // These constants are used as FileInfo keys
  static const std::string COMPRESSION;
  static const std::string COMPRESSION_COMPACT;
  static const std::string ENCODE_ON_DISK;
  static const std::string DATA_BLOCK_ENCODING;
  /**
   * Key for the BLOCKCACHE attribute.
   * A more exact name would be CACHE_DATA_ON_READ because this flag sets whether or not we
   * cache DATA blocks.  We always cache INDEX and BLOOM blocks; caching these blocks cannot be
   * disabled.
   */
  static const std::string BLOCKCACHE;
  static const std::string CACHE_DATA_ON_WRITE;
  static const std::string CACHE_INDEX_ON_WRITE;
  static const std::string CACHE_BLOOMS_ON_WRITE;
  static const std::string EVICT_BLOCKS_ON_CLOSE;
  /**
   * Key for cache data into L1 if cache is set up with more than one tier.
   * To set in the shell, do something like this:
   * <code>hbase(main):003:0&gt; create 't',
   *    {NAME; 'true'}}</code>
   */
  static const std::string CACHE_DATA_IN_L1;

  /**
   * Key for the PREFETCH_BLOCKS_ON_OPEN attribute.
   * If set, all INDEX, BLOOM, and DATA blocks of HFiles belonging to this
   * family will be loaded into the cache as soon as the file is opened. These
   * loads will not count as cache misses.
   */
  static const std::string PREFETCH_BLOCKS_ON_OPEN;

  /**
   * Size of storefile/hfile 'blocks'.  Default is {@link #DEFAULT_BLOCKSIZE}.
   * Use smaller block sizes for faster random-access at expense of larger
   * indices (more memory consumption).
   */
  static const std::string BLOCKSIZE;
  static const std::string LENGTH;
  static const std::string TTL;
  static const std::string BLOOMFILTER;
  static const std::string FOREVER;
  static const std::string REPLICATION_SCOPE;
  static const BYTE_ARRAY REPLICATION_SCOPE_BYTES;
  static const std::string MIN_VERSIONS;
  /**
   * Retain all cells across flushes and compactions even if they fall behind
   * a delete tombstone. To see all retained cells, do a 'raw' scan; see
   * Scan#setRaw or pass RAW; true attribute in the shell.
   */
  static const std::string KEEP_DELETED_CELLS;
  static const std::string COMPRESS_TAGS;

  static const std::string ENCRYPTION;
  static const std::string ENCRYPTION_KEY;

  static const std::string IS_MOB;
  static const BYTE_ARRAY IS_MOB_BYTES;
  static const std::string MOB_THRESHOLD;
  static const BYTE_ARRAY MOB_THRESHOLD_BYTES;
  static const long DEFAULT_MOB_THRESHOLD; // 100k

  static const std::string DFS_REPLICATION;
  static const short DEFAULT_DFS_REPLICATION;

  /**
   * Default compression type.
   */
  static const std::string DEFAULT_COMPRESSION;

  /**
   * Default value of the flag that enables data block encoding on disk, as
   * opposed to encoding in cache only. We encode blocks everywhere by default,
   * as long as {@link #DATA_BLOCK_ENCODING} is not NONE.
   */
  static const bool DEFAULT_ENCODE_ON_DISK;

  /** Default data block encoding algorithm. */
  static const std::string DEFAULT_DATA_BLOCK_ENCODING;

  /**
   * Default number of versions of a record to keep.
   */
  static const int DEFAULT_VERSIONS;

  /**
   * Default is not to keep a minimum of versions.
   */
  static const int DEFAULT_MIN_VERSIONS;

  /**
   * Default setting for whether to try and serve this column family from memory or not.
   */
  static const bool DEFAULT_IN_MEMORY;

  /**
   * Default setting for preventing deleted from being collected immediately.
   */
  static const HBaseConstants::KEEP_DELETED_CELLS DEFAULT_KEEP_DELETED;

  /**
   * Default setting for whether to use a block cache or not.
   */
  static const bool DEFAULT_BLOCKCACHE;

  /**
   * Default setting for whether to cache data blocks on write if block caching
   * is enabled.
   */
  static const bool DEFAULT_CACHE_DATA_ON_WRITE;

  /**
   * Default setting for whether to cache data blocks in L1 tier.  Only makes sense if more than
   * one tier in operations: i.e. if we have an L1 and a L2.  This will be the cases if we are
   * using BucketCache.
   */
  static const bool DEFAULT_CACHE_DATA_IN_L1;

  /**
   * Default setting for whether to cache index blocks on write if block
   * caching is enabled.
   */
  static const bool DEFAULT_CACHE_INDEX_ON_WRITE;

  /**
   * Default size of blocks in files stored to the filesytem (hfiles).
   */
  static const int DEFAULT_BLOCKSIZE;

  /**
   * Default setting for whether or not to use bloomfilters.
   */
  static const std::string DEFAULT_BLOOMFILTER;

  /**
   * Default setting for whether to cache bloom filter blocks on write if block
   * caching is enabled.
   */
  static const bool DEFAULT_CACHE_BLOOMS_ON_WRITE;

  /**
   * Default time to live of cell contents.
   */
  static const int DEFAULT_TTL;

  /**
   * Default scope.
   */
  static const int DEFAULT_REPLICATION_SCOPE;

  /**
   * Default setting for whether to evict cached blocks from the blockcache on
   * close.
   */
  static const bool DEFAULT_EVICT_BLOCKS_ON_CLOSE;

  /**
   * Default compress tags along with any type of DataBlockEncoding.
   */
  static const bool DEFAULT_COMPRESS_TAGS;

  /*
   * Default setting for whether to prefetch blocks into the blockcache on open.
   */
  static const bool DEFAULT_PREFETCH_BLOCKS_ON_OPEN;
  static const std::string VERSIONS;
  static const std::string IN_MEMORY;

  ColumnFamilySchema();
  ColumnFamilySchema(const std::string &family_name);
  ColumnFamilySchema(const BYTE_ARRAY &family_name);
  virtual ~ColumnFamilySchema();

  bool IsLegalFamilyName(const BYTE_ARRAY &family_name);
  ColumnFamilySchema SetMaxVersions(const int &max_versions);
  ColumnFamilySchema SetMinVersions(const int &min_versions);
  ColumnFamilySchema SetKeepDeletedCells(const HBaseConstants::KEEP_DELETED_CELLS &keep_deleted_cells);
  ColumnFamilySchema SetInMemory(const bool &set_in_memory);
  ColumnFamilySchema SetBlockCacheEnabled(const bool &set_block_cache);
  ColumnFamilySchema SetTimeToLive(const int &ttl);
  ColumnFamilySchema SetCompressionType(const HBaseConstants::COMPRESSION_ALGORITHM &compression_algorithm);
  ColumnFamilySchema SetDataBlockEncoding(const HBaseConstants::DATA_BLOCK_ENCODING &data_block_encoding);
  ColumnFamilySchema SetBloomFilterType(const HBaseConstants::BLOOM_TYPE &bloom_type);
  ColumnFamilySchema SetBlocksize(const int &default_blocksize);
  ColumnFamilySchema SetScope(const int &replication_scope);

  ColumnFamilySchema SetValue(const std::string &key, const std::string &value);
  ColumnFamilySchema SetValue(const BYTE_ARRAY &key, const BYTE_ARRAY &value);
  ColumnFamilySchema RemoveValue(const std::string &key);
  ColumnFamilySchema RemoveValue(const BYTE_ARRAY &key);

  ColumnFamilySchema SetConfiguration(const std::string &key, const std::string &value);
  ColumnFamilySchema RemoveConfiguration(const std::string &key);

  std::string GetValue(const std::string &key);
  BYTE_ARRAY GetValue(const BYTE_ARRAY &key);
  int GetMinVersions();
  const BYTE_ARRAY &GetName() const;
  const std::string GetNameAsString() const;
  const std::map<Bytes*, Bytes*> &GetValues() const;
  const std::map<std::string, std::string> &GetConfiguration() const;

  static std::vector <Bytes> PopulateStaticVals();
  static std::map<std::string, std::string> PopulateDefaultValues();

 private:
  static const std::map<std::string, std::string> DEFAULT_VALUES;
  static const std::vector <Bytes> RESERVED_KEYWORDS;
  static const int UNINITIALIZED;

  // Column family name
  BYTE_ARRAY family_name_;
  // Column metadata
  std::map<Bytes*, Bytes*> column_metadata_values_;
  /**
   * A map which holds the configuration specific to the column family.
   * The keys of the map have the same names as config keys and override the defaults with
   * cf-specific settings. Example usage may be for compactions, etc.
   */
  std::map<std::string, std::string> column_configuration_;
  /*
   * Cache the max versions rather than calculate it every time.
   */
  int cached_max_versions_;

};

