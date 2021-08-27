/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.grpc.MetricCollectingServerInterceptor;
import io.micronaut.configuration.metrics.annotation.RequiresMetrics;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;

@Factory
@RequiresMetrics
public class MetricCollectingServerInterceptorFactory {

  @Bean
  MetricCollectingServerInterceptor metricCollectingServerInterceptor(MeterRegistry meterRegistry) {
    return new MetricCollectingServerInterceptor(meterRegistry);
  }

}
