/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream.metrics;

import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
class CommonTagsMeterFilter implements MeterFilter {

  private static final Set<String> STAGES = Set.of("production", "staging");
  private static final Logger logger = LoggerFactory.getLogger(CommonTagsMeterFilter.class);

  private final Tags commonTags;

  CommonTagsMeterFilter(@Value("${datadog-environment}") String datadogEnvironment) {

    final String version = AccountStreamVersion.getVersion();
    logger.info("v{} in {}", version, datadogEnvironment);

    commonTags = Tags.of(
        "service", GlobalMeterRegistryConfigurer.SERVICE,
        "version", version,
        "env", datadogEnvironment,
        "host", HostnameUtil.getLocalHostname()
    );
  }

  /**
   * Remaps an existing `service` tag to `serviceName`, to handle {@link io.micrometer.core.instrument.binder.grpc.MetricCollectingServerInterceptor}
   * behavior
   * <p>
   * Also, subtly different from {@link MeterFilter#commonTags(Iterable)}, as we want common tags to override any values
   * set on the {@code id}.
   */
  @Override
  public Id map(final Id id) {
    if (id.getName().startsWith("grpc")) {
      // metrics from `MetricCollectingServerInterceptor` already have a `service` tag, which we want to preserve
      final String serviceTag = id.getTag("service");

      if (serviceTag != null) {
        id.replaceTags(Tags.concat(id.getTagsAsIterable(), "serviceName", serviceTag));
      }
    }

    return id.replaceTags(Tags.concat(id.getTagsAsIterable(), commonTags));
  }
}
