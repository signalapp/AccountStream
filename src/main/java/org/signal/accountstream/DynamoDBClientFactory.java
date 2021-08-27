/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Factory for building a DynamoDB client.  We don't use Micronaut's version of this,
 * as it doesn't appear to work within a Fargate context.
 */
@Factory
public class DynamoDBClientFactory {

  @Value("${aws.region}")
  String region;

  @Singleton
  DynamoDbClient dynamoDbClient() {
    return DynamoDbClient.builder()
        .region(Region.of(region))
        .build();
  }
}
