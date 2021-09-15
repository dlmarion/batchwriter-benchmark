/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.dlmarion;

import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.Assert.fail;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 5)
@Measurement(iterations = 20)
@Fork(1)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class MyBenchmarkIT extends SharedMiniClusterBase implements MiniClusterConfigurationCallback {

  public static final String TABLE_NAME = MyBenchmarkIT.class.getSimpleName();
  
  @Param({"1048576" /*, "10485760", "104857600", "1073741824"*/})
  public String memory;
  
  @Param({/*"1024",*/ "10240"/*, "102400"*/})
  public String mutationSize;
  
  @Param({/*"10", "100", */"1000"/*, "10000"*/})
  public String numMutations;

  private Iterator<Mutation> mutations = null;
  
  private static final int NUM_TSERVERS = 10;
  
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, Boolean.FALSE.toString());
      cfg.setProperty(Property.TSERV_PORTSEARCH, Boolean.TRUE.toString());
      // use raw local file system so walogs sync and flush will work
      coreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    }

    @Setup
    public void setup() throws Exception {
      
      if (SharedMiniClusterBase.getCluster() == null) {
        SharedMiniClusterBase.startMiniClusterWithConfig(this);
      }
      
      // Stop the tablet servers
      getCluster().getProcesses().get(TABLET_SERVER).forEach(p -> {
        try {
          getCluster().killProcess(TABLET_SERVER, p);
        } catch (Exception e) {
          fail("Failed to shutdown tablet server");
        }
      });
      
      // Start our TServer that will not commit the compaction
      for (int i = 0; i < NUM_TSERVERS; i++) {
        SharedMiniClusterBase.getCluster().exec(BenchmarkTabletServer.class);
      }

      Integer count = Integer.parseInt(numMutations);
      List<Mutation> muts = new ArrayList<>(count);
      
      SecureRandom random = new SecureRandom();
      
      TreeSet<Text> splits = new TreeSet<>();
      Integer size = Integer.parseInt(mutationSize);
      byte[] value = new byte[size];
      random.nextBytes(value);
      // Create the mutations
      for (int i = 0; i < count; i++) {
        byte[] row = new byte[i];
        byte[] colf = new byte[i];
        byte[] colq = new byte[i];
        random.nextBytes(row);
        random.nextBytes(colf);
        random.nextBytes(colq);
        Mutation m = new Mutation(row);
        m.put(colf, colq, System.currentTimeMillis(), value);
        muts.add(m);
        if (count % NUM_TSERVERS == 0) {
          splits.add(new Text(row));
        }
      }
      client = Accumulo.newClient().from(SharedMiniClusterBase.getCluster().getClientProperties()).build();
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.withSplits(splits);
      client.tableOperations().create(TABLE_NAME, ntc);
      client.instanceOperations().waitForBalance();
      
      mutations = muts.iterator();
      BatchWriterConfig bwConfig = new BatchWriterConfig();
      bwConfig.setDurability(Durability.DEFAULT);
      bwConfig.setTimeout(0, TimeUnit.MILLISECONDS);
      bwConfig.setMaxLatency(10, TimeUnit.SECONDS);
      bwConfig.setMaxMemory(Long.parseLong(memory));
      bwConfig.setMaxWriteThreads(NUM_TSERVERS);
      writer = client.createBatchWriter(TABLE_NAME, bwConfig);
    }
    
    private AccumuloClient client = null;
    private BatchWriter writer = null;

    @Benchmark
    public void testMethod() throws Exception {
      while (mutations.hasNext()) {
        writer.addMutation(mutations.next());
      }
      writer.flush();
    }
    
    @TearDown
    public void tearDown() throws Exception {
      writer.close();
      client.close();
      boolean dataWasInserted = false;
      try (AccumuloClient client = Accumulo.newClient().from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {
        Scanner s = client.createScanner(TABLE_NAME);
        Iterator<Entry<Key,Value>> iter = s.iterator();
        if (iter.hasNext()) {
          dataWasInserted = true;
        }
        s.close();
        client.tableOperations().delete(TABLE_NAME);
      }
      SharedMiniClusterBase.stopMiniCluster();
      if (dataWasInserted) {
        throw new RuntimeException("Data was inserted into the table.");
      }
    }

}
