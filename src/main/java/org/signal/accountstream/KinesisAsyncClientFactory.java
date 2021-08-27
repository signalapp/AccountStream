/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

@Factory
public class KinesisAsyncClientFactory {

  @Value("${aws.region}")
  String region;

  @Singleton
  KinesisAsyncClient kinesisAsyncClient() {
    return KinesisAsyncClient.builder()
        .region(Region.of(region))
        .build();
  }
}
