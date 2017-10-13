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
package org.apache.hadoop.hbase.procedure2.store.wal;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({SmallTests.class})
public class TestProcedureWALFormatReader {
  private ProcedureWALFormatReader reader;

  @Before
  public void setup() throws IOException {
    this.reader = Mockito.mock(ProcedureWALFormatReader.class);
    Mockito.doCallRealMethod().when(reader).deserializeAndStoreProcedure(
        Mockito.anyMap(), Mockito.any(ProcedureProtos.Procedure.class), Mockito.anyLong());
  }
  
  @Test
  public void testKnownProcedures() throws IOException {
    Map<Long,Procedure> procedures = new HashMap<>();
    reader.deserializeAndStoreProcedure(procedures, getProcedure(
        "org.apache.hadoop.hbase.master.procedure.CreateTableProcedure"), 1L);
    reader.deserializeAndStoreProcedure(procedures, getProcedure(
        "org.apache.hadoop.hbase.master.procedure.AddColumnFamilyProcedure"), 2L);
    reader.deserializeAndStoreProcedure(procedures, getProcedure(
        "org.apache.hadoop.hbase.master.procedure.DisableTableProcedure"), 3L);
    reader.deserializeAndStoreProcedure(procedures, getProcedure(
        "org.apache.hadoop.hbase.master.procedure.DropTableProcedure"), 4L);
    assertEquals(4, procedures.size());
    assertEquals(new HashSet<Long>(Arrays.asList(1L, 2L, 3L, 4L)), procedures.keySet());
  }

  @Test
  public void testSomeUnknownProcedures() throws IOException {
    Map<Long,Procedure> procedures = new HashMap<>();
    reader.deserializeAndStoreProcedure(procedures, getProcedure(
        "org.apache.hadoop.hbase.master.procedure.CreateTableProcedure"), 1L);
    reader.deserializeAndStoreProcedure(procedures, getProcedure(
        "org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure"), 2L);
    reader.deserializeAndStoreProcedure(procedures, getProcedure(
        "org.apache.hadoop.hbase.master.procedure.AddColumnFamilyProcedure"), 3L);
    reader.deserializeAndStoreProcedure(procedures, getProcedure(
        "org.apache.hadoop.hbase.master.procedure.DisableTableProcedure"), 4L);
    reader.deserializeAndStoreProcedure(procedures, getProcedure(
        "org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure"), 5L);
    reader.deserializeAndStoreProcedure(procedures, getProcedure(
        "org.apache.hadoop.hbase.master.procedure.DropTableProcedure"), 6L);
    assertEquals(4, procedures.size());
    assertEquals(new HashSet<Long>(Arrays.asList(1L, 3L, 4L, 6L)), procedures.keySet());
    
  }

  ProcedureProtos.Procedure getProcedure(String className) {
    return ProcedureProtos.Procedure.newBuilder()
        .setClassName(Objects.requireNonNull(className))
        .setLastUpdate(10)
        .setProcId(1)
        .setStartTime(2)
        .setState(ProcedureProtos.ProcedureState.FINISHED)
        .build();
  }
}
