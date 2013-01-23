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
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.io.VersionedWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This is a class that extends HServerLoad with last sequence IDs.
 * It enables us to port 6508 w/o breaking compat.
 */
public class HServerLoadWithSeqIds extends VersionedWritable {
  private static final byte VERSION = 1;

  private HServerLoad serverLoad;

  public HServerLoadWithSeqIds() {
    super();
  }

  public HServerLoadWithSeqIds(HServerLoad serverLoad) {
    super();
    this.serverLoad = serverLoad;
  }

  /** @return the object version number */
  @Override
  public byte getVersion() {
    return VERSION;
  }

  public HServerLoad getServerLoad() {
    return serverLoad;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    serverLoad.write(out);
    // HACK: this loop MUST write regions in the same order as HServerLoad.write.
    for (HServerLoad.RegionLoad rl: serverLoad.regionLoad.values()) {
      out.writeLong(rl.getCompleteSequenceId());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    if (null == serverLoad) {
      serverLoad = new HServerLoad();
    }
    List<byte[]> regionKeys = serverLoad.readFieldsGetRegionKeys(in);
    for (int i = 0; i < regionKeys.size(); ++i) {
      long lastSequenceId = in.readLong();
      HServerLoad.RegionLoad rl = serverLoad.regionLoad.get(regionKeys.get(i));
      rl.setCompleteSequenceId(lastSequenceId);
    }
  }
}
