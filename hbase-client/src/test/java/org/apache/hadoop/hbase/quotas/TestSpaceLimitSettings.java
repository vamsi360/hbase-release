/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.SpaceLimitRequest;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.SpaceQuota;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test class for {@link SpaceLimitSettings}.
 */
@Category({SmallTests.class})
public class TestSpaceLimitSettings {

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTableQuotaSizeLimit() {
    new SpaceLimitSettings(TableName.valueOf("foo"), -1, SpaceViolationPolicy.NO_INSERTS);
  }

  @Test(expected = NullPointerException.class)
  public void testNullTableName() {
    TableName tn = null;
    new SpaceLimitSettings(tn, 1, SpaceViolationPolicy.NO_INSERTS);
  }

  @Test(expected = NullPointerException.class)
  public void testNullTableViolationPolicy() {
    new SpaceLimitSettings(TableName.valueOf("foo"), 1, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNamespaceQuotaSizeLimit() {
    new SpaceLimitSettings("foo_ns", -1, SpaceViolationPolicy.NO_INSERTS);
  }

  @Test(expected = NullPointerException.class)
  public void testNullNamespace() {
    String ns = null;
    new SpaceLimitSettings(ns, 1, SpaceViolationPolicy.NO_INSERTS);
  }

  @Test(expected = NullPointerException.class)
  public void testNullNamespaceViolationPolicy() {
    new SpaceLimitSettings("foo_ns", 1, null);
  }

  @Test
  public void testTableQuota() {
    final TableName tableName = TableName.valueOf("foo");
    final long sizeLimit = 1024 * 1024;
    final SpaceViolationPolicy policy = SpaceViolationPolicy.NO_WRITES;
    SpaceLimitSettings settings = new SpaceLimitSettings(tableName, sizeLimit, policy);
    SetQuotaRequest proto = QuotaSettings.buildSetQuotaRequestProto(settings);

    assertFalse("User should be missing", proto.hasUserName());
    assertFalse("Namespace should be missing", proto.hasNamespace());
    assertEquals(ProtobufUtil.toProtoTableName(tableName), proto.getTableName());
    SpaceLimitRequest spaceLimitReq = proto.getSpaceLimit();
    assertNotNull("SpaceLimitRequest was null", spaceLimitReq);
    SpaceQuota spaceQuota = spaceLimitReq.getQuota();
    assertNotNull("SpaceQuota was null", spaceQuota);
    assertEquals(sizeLimit, spaceQuota.getSoftLimit());
    assertEquals(ProtobufUtil.toProtoViolationPolicy(policy), spaceQuota.getViolationPolicy());

    assertEquals(QuotaType.SPACE, settings.getQuotaType());

    SpaceLimitSettings copy = new SpaceLimitSettings(tableName, sizeLimit, policy);
    assertEquals(settings, copy);
    assertEquals(settings.hashCode(), copy.hashCode());
  }

  @Test
  public void testNamespaceQuota() {
    final String namespace = "foo_ns";
    final long sizeLimit = 1024 * 1024;
    final SpaceViolationPolicy policy = SpaceViolationPolicy.NO_WRITES;
    SpaceLimitSettings settings = new SpaceLimitSettings(namespace, sizeLimit, policy);
    SetQuotaRequest proto = QuotaSettings.buildSetQuotaRequestProto(settings);

    assertFalse("User should be missing", proto.hasUserName());
    assertFalse("TableName should be missing", proto.hasTableName());
    assertEquals(namespace, proto.getNamespace());
    SpaceLimitRequest spaceLimitReq = proto.getSpaceLimit();
    assertNotNull("SpaceLimitRequest was null", spaceLimitReq);
    SpaceQuota spaceQuota = spaceLimitReq.getQuota();
    assertNotNull("SpaceQuota was null", spaceQuota);
    assertEquals(sizeLimit, spaceQuota.getSoftLimit());
    assertEquals(ProtobufUtil.toProtoViolationPolicy(policy), spaceQuota.getViolationPolicy());

    assertEquals(QuotaType.SPACE, settings.getQuotaType());

    SpaceLimitSettings copy = new SpaceLimitSettings(namespace, sizeLimit, policy);
    assertEquals(settings, copy);
    assertEquals(settings.hashCode(), copy.hashCode());
  }
}
