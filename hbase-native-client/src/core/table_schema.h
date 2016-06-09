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
#include <string>
#include <vector>

#include "bytes.h"
#include "column_family_schema.h"
#include "hconstants.h"
#include "mutation.h"
#include "table_name.h"

class TableSchema {
 public:
  static const std::string SPLIT_POLICY;

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata
   * attribute which denotes the maximum size of the store file after which
   * a region split occurs
   *
   * @see #getMaxFileSize()
   */
  static const std::string MAX_FILESIZE;
  static const std::string OWNER;
  static const BYTE_ARRAY OWNER_KEY;

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata
   * attribute which denotes if the table is Read Only
   *
   * @see #isReadOnly()
   */
  static const std::string READONLY;

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata
   * attribute which denotes if the table is compaction enabled
   *
   * @see #isCompactionEnabled()
   */
  static const std::string COMPACTION_ENABLED;

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata
   * attribute which represents the maximum size of the memstore after which
   * its contents are flushed onto the disk
   *
   * @see #getMemStoreFlushSize()
   */
  static const std::string MEMSTORE_FLUSHSIZE;

  static const std::string FLUSH_POLICY;

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata
   * attribute which denotes if the table is a -ROOT- region or not
   *
   * @see #isRootRegion()
   */
  static const std::string IS_ROOT;

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata
   * attribute which denotes if it is a catalog table, either
   * <code> hbase:meta </code> or <code> -ROOT- </code>
   *
   * @see #isMetaRegion()
   */
  static const std::string IS_META;

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata
   * attribute which denotes if the deferred log flush option is enabled.
   * @deprecated Use {@link #DURABILITY} instead.
   */
  static const std::string DEFERRED_LOG_FLUSH;

  /**
   * <em>INTERNAL</em> {@link Durability} setting for the table.
   */
  static const std::string DURABILITY;

  /**
   * <em>INTERNAL</em> number of region replicas for the table.
   */
  static const std::string REGION_REPLICATION;

  /**
   * <em>INTERNAL</em> flag to indicate whether or not the memstore should be replicated
   * for read-replicas (CONSISTENCY =&gt; TIMELINE).
   */
  static const std::string REGION_MEMSTORE_REPLICATION;

  /**
   * <em>INTERNAL</em> Used by shell/rest interface to access this metadata
   * attribute which denotes if the table should be treated by region normalizer.
   *
   * @see #isNormalizationEnabled()
   */
  static const std::string NORMALIZATION_ENABLED;


  /**
   * Constant that denotes whether the table is READONLY by default and is false
   */
  static const bool DEFAULT_READONLY;

  /**
   * Constant that denotes whether the table is compaction enabled by default
   */
  static const bool DEFAULT_COMPACTION_ENABLED;

  /**
   * Constant that denotes whether the table is normalized by default.
   */
  static const bool DEFAULT_NORMALIZATION_ENABLED;

  /**
   * Constant that denotes the maximum default size of the memstore after which
   * the contents are flushed to the store files
   */
  static const long DEFAULT_MEMSTORE_FLUSH_SIZE;

  static const int DEFAULT_REGION_REPLICATION;

  static const bool DEFAULT_REGION_MEMSTORE_REPLICATION;

  std::map<std::string, std::string> tableAttributes;
  std::vector<ColumnFamilySchema> colFamily;
  std::map<std::string, std::string> tableConfig;

  TableSchema();
  TableSchema(TableName *table_name);
  TableSchema(const TableSchema &ctable_schema);
  TableSchema& operator= (const TableSchema &ctable_schema);
  virtual ~TableSchema();
  TableSchema AddFamily(const ColumnFamilySchema &family);

  TableSchema SetValue(const std::string &key, const std::string &value);
  TableSchema SetValue(const BYTE_ARRAY &key, const BYTE_ARRAY &value);
  TableSchema RemoveValue(const std::string &key);
  TableSchema RemoveValue(const BYTE_ARRAY &key);

  TableSchema SetConfiguration(const std::string &key, const std::string &value);
  TableSchema RemoveConfiguration(const std::string &key);

  const std::map<BYTE_ARRAY, ColumnFamilySchema> &GetFamily() const;
  const TableName &GetTableName() const;
  const std::map<Bytes*, Bytes*> &GetMetadata() const;
  const std::map<std::string, std::string> &GetConfiguration() const;

  static std::map<std::string, std::string> PopulateDefaultValues();
  static std::vector <Bytes> PopulateStaticVals();

 private:

  static const BYTE_ARRAY MAX_FILESIZE_KEY;
  static const BYTE_ARRAY READONLY_KEY;
  static const BYTE_ARRAY COMPACTION_ENABLED_KEY;
  static const BYTE_ARRAY MEMSTORE_FLUSHSIZE_KEY;
  static const BYTE_ARRAY IS_ROOT_KEY;
  static const BYTE_ARRAY IS_META_KEY;
  static const BYTE_ARRAY DEFERRED_LOG_FLUSH_KEY;
  static const BYTE_ARRAY DURABILITY_KEY;
  static const BYTE_ARRAY REGION_REPLICATION_KEY;
  static const BYTE_ARRAY REGION_MEMSTORE_REPLICATION_KEY;
  static const BYTE_ARRAY NORMALIZATION_ENABLED_KEY;
  /** Default durability for HTD is USE_DEFAULT, which defaults to HBase-global default value */
  static const HBaseConstants::DURABILITY_TYPE DEFAULT_DURABLITY;
  /*
   *  The below are ugly but better than creating them each time till we
   *  replace booleans being saved as std::strings with plain booleans.  Need a
   *  migration script to do this.  TODO.
   */
  static const BYTE_ARRAY FALSE;
  static const BYTE_ARRAY TRUE;
  static const bool DEFAULT_DEFERRED_LOG_FLUSH;

  static const std::map<std::string, std::string> DEFAULT_VALUES;
  static const std::vector<BYTE_ARRAY> RESERVED_KEYWORDS;

  TableName *table_name_;
  /**
   * A map which holds the metadata information of the table. This metadata
   * includes values like IS_ROOT, IS_META, DEFERRED_LOG_FLUSH, SPLIT_POLICY,
   * MAX_FILE_SIZE, READONLY, MEMSTORE_FLUSHSIZE etc...
   */
  std::map<Bytes*, Bytes*> table_metadata_values_;
  /**
   * A map which holds the configuration specific to the table.
   * The keys of the map have the same names as config keys and override the defaults with
   * table-specific settings. Example usage may be for compactions, etc.
   */
  std::map<std::string, std::string> table_configuration_;
  /**
   * Maps column family name to the respective ColumnFamilySchemas
   */
  std::map<BYTE_ARRAY, ColumnFamilySchema> families_;

};
