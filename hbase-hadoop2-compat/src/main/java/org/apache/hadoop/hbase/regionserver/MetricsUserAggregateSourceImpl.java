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

package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

@InterfaceAudience.Private
public class MetricsUserAggregateSourceImpl extends BaseSourceImpl
  implements MetricsUserAggregateSource {

  private static final Log LOG = LogFactory.getLog(MetricsUserAggregateSourceImpl.class);

  private final ConcurrentHashMap<String, MetricsUserSource> userSources =
      new ConcurrentHashMap<String, MetricsUserSource>();

  public MetricsUserAggregateSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsUserAggregateSourceImpl(String metricsName,
      String metricsDescription,
      String metricsContext,
      String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public MetricsUserSource getOrCreateMetricsUser(String user) {
    MetricsUserSource source = userSources.get(user);
    if (source != null) {
      return source;
    }
    source = new MetricsUserSourceImpl(user, this);
    MetricsUserSource prev = userSources.putIfAbsent(user, source);

    if (prev != null) {
      return prev;
    } else {
      // register the new metrics now
      register(source);
    }
    return source;
  }

  public void register(MetricsUserSource source) {
    synchronized (this) {
      source.register();
    }
  }

  public void deregister(MetricsUserSource toRemove) {
    /*
     * We do not deregister per-user level metrics for now, so this function is not used. The user
     * can go away and come back hours or days later, but we still would like to keep context for
     * the user to calculate the counters and rates. Users are usually static in a cluster and
     * there are not thousands of them, so it should be fine to not do the cleanup.
     *
     * Since we are not removing metrics, JmxCacheBuster is also not called periodically.
     */
    try {
      synchronized (this) {
        MetricsUserSource source = userSources.remove(toRemove.getUser());
        if (source != null) {
          source.deregister();
        }
      }
    } catch (Exception e) {
      // Ignored. If this errors out it means that someone is double
      // closing the user source and the user metrics is already nulled out.
      LOG.info( "Error trying to remove " + toRemove + " from " + getClass().getSimpleName(), e);
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder mrb = collector.addRecord(metricsName);

    if (userSources != null) {
      for (MetricsUserSource userMetricSource : userSources.values()) {
        if (userMetricSource instanceof MetricsUserSourceImpl) {
          ((MetricsUserSourceImpl) userMetricSource).snapshot(mrb, all);
        }
      }
      mrb.addGauge(Interns.info(NUM_USERS, NUMBER_OF_USERS_DESC), userSources.size());
      metricsRegistry.snapshot(mrb, all);
    }
  }
}