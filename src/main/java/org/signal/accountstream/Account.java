/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

public class Account {
  public final long e164;
  public final UUID uuid;
  public final boolean canonicallyDiscoverable;
  public final UUID pni;
  public final byte[] uak;

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    OBJECT_MAPPER
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false);
  }

  public static Account fromInputStream(InputStream inputStream) throws IOException {
    return OBJECT_MAPPER.readValue(inputStream, Account.class);
  }

  @JsonCreator
  public Account(
      @JsonProperty("e164") final long e164,
      @JsonProperty("uuid") final UUID uuid,
      @JsonProperty("canonicallyDiscoverable") final boolean canonicallyDiscoverable,
      @JsonProperty("pni") final UUID pni,
      @JsonProperty("uak") final byte[] uak) {
    this.e164 = e164;
    this.uuid = uuid;
    this.canonicallyDiscoverable = canonicallyDiscoverable;
    this.pni = pni;
    this.uak = uak;
  }
}
