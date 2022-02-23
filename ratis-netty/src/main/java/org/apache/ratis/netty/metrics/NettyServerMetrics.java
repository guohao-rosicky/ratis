/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.netty.metrics;

import com.codahale.metrics.Timer;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;

public class NettyServerMetrics extends RatisMetrics {
  private static final String RATIS_NETTY_METRICS_APP_NAME = "ratis_stream";
  private static final String RATIS_NETTY_METRICS_COMP_NAME = "netty_server";
  private static final String RATIS_NETTY_METRICS_DESC = "Metrics for Ratis Stream Netty Server";

  public static final String RATIS_NETTY_METRICS_RPC_LATENCY =
      "%s_latency";
  public static final String RATIS_NETTY_METRICS_RPC_SUCCESS =
      "%s_success_reply_count";
  public static final String RATIS_NETTY_METRICS_RPC_FAIL =
      "%s_fail_reply_count";

  public static final String HEADER_REQUEST = "header";

  public static final String LOCAL_WRITE_REQUEST = "local_write";
  public static final String REMOTE_WRITE_REQUEST = "remote_write";

  public static final String INCOMING_REQUEST = "incoming_request";
  public static final String START_TRANSACTION_REQUEST = "start_transaction";

  public static final String RATIS_NETTY_METRICS_REQUESTS_TOTAL = "num_requests";

  public NettyServerMetrics(String serverId) {
    registry = getMetricRegistryForGrpcServer(serverId);
  }

  private RatisMetricRegistry getMetricRegistryForGrpcServer(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATIS_NETTY_METRICS_APP_NAME,
        RATIS_NETTY_METRICS_COMP_NAME, RATIS_NETTY_METRICS_DESC));
  }

  public Timer getNettyRpcLatencyTimer(String requestOpt) {
    return registry.timer(String.format(RATIS_NETTY_METRICS_RPC_LATENCY, requestOpt));
  }

  public void onRequestCreate(String requestOpt) {
    registry.counter(RATIS_NETTY_METRICS_REQUESTS_TOTAL + "_" + requestOpt).inc();
  }

  public void onRequestSuccess(String requestOpt) {
    registry.counter(String.format(RATIS_NETTY_METRICS_RPC_SUCCESS, requestOpt)).inc();
  }

  public void onRequestFail(String requestOpt) {
    registry.counter(String.format(RATIS_NETTY_METRICS_RPC_FAIL, requestOpt)).inc();
  }

  @VisibleForTesting
  public RatisMetricRegistry getRegistry() {
    return registry;
  }
}
