/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.signal.lambda;

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

class Account {

  @VisibleForTesting
  static final String KEY_ACCOUNT_UUID = "U";
  @VisibleForTesting
  static final String ATTR_ACCOUNT_E164 = "P";
  @VisibleForTesting
  static final String ATTR_CANONICALLY_DISCOVERABLE = "C";
  @VisibleForTesting
  static final String ATTR_ACCOUNT_DATA = "D";

  @JsonProperty
  String e164;

  @JsonProperty
  byte[] uuid;

  @JsonProperty
  boolean canonicallyDiscoverable;

  @JsonProperty
  byte[] pni;

  @JsonProperty
  byte[] uak;

  Account() {
  }  // empty constructor for JSON

  private static class AccountDataJson {
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
      OBJECT_MAPPER
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, false)
          .configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false);
    }

    private AccountDataJson() {}

    static AccountDataJson fromItem(Map<String, AttributeValue> item) {
      return Optional.ofNullable(item.get(ATTR_ACCOUNT_DATA))
          .map(d -> d.getB())
          .map(b -> {
            try {
              return OBJECT_MAPPER.readValue(new ByteBufferInputStream(b), AccountDataJson.class);
            } catch (IOException e) {
              return null;
            }
          })
          .orElse(new AccountDataJson());
    }

    @JsonProperty
    byte[] uak = null;
    @JsonProperty
    UUID pni = null;
  }

  Account(String e164, byte[] uuid, boolean canonicallyDiscoverable, byte[] pni, byte[] uak) {
    this.e164 = e164;
    this.uuid = uuid;
    this.canonicallyDiscoverable = canonicallyDiscoverable;
    this.pni = pni;
    this.uak = uak;
  }

  public static byte[] uuidToBytes(UUID uuid) {
    if (uuid == null) return null;
    ByteBuffer b = ByteBuffer.allocate(16);
    b.putLong(uuid.getMostSignificantBits());
    b.putLong(uuid.getLeastSignificantBits());
    return b.array();
  }

  static Account fromItem(Map<String, AttributeValue> item) {
    Preconditions.checkNotNull(item.get(KEY_ACCOUNT_UUID));
    Preconditions.checkNotNull(item.get(ATTR_ACCOUNT_E164));
    AccountDataJson json = AccountDataJson.fromItem(item);
    byte[] uuid = new byte[16];
    item.get(KEY_ACCOUNT_UUID).getB().get(uuid);
    return new Account(
        item.get(ATTR_ACCOUNT_E164).getS(),
        uuid,
        Optional.ofNullable(item.get(ATTR_CANONICALLY_DISCOVERABLE)).map(x -> x.getBOOL().booleanValue())
            .orElse(false),
        uuidToBytes(json.pni),
        json.uak);
  }

  Account forceNotInCds() {
    return new Account(e164, uuid, false, pni, uak);
  }

  // Partition such that the primary key (UUID) is maintained across streams.
  String partitionKey() {
    ByteBuffer s = ByteBuffer.wrap(uuid);
    return new UUID(s.getLong(), s.getLong()).toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    Account account = (Account) o;
    return canonicallyDiscoverable == account.canonicallyDiscoverable &&
        e164.equals(account.e164) &&
        Arrays.equals(uuid, account.uuid) &&
        Arrays.equals(pni, account.pni) &&
        Arrays.equals(uak, account.uak);
  }

  @Override
  public String toString() {
    return "Account{" +
        "e164='" + e164 +
        "', uuid=" + Arrays.toString(uuid) +
        ", canonicallyDiscoverable=" + canonicallyDiscoverable +
        ", uak=" + Arrays.toString(uak) +
        ", pni=" + Arrays.toString(pni) +
        "}";
  }
}
