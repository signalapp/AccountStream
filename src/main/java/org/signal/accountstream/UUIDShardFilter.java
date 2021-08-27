/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import java.util.UUID;

public class UUIDShardFilter {
  private final UUID fromInclusive;
  private final UUID toExclusive;

  public UUIDShardFilter(UUID fromInclusive, UUID toExclusive) {
    if (fromInclusive != null && toExclusive != null && compare(fromInclusive, toExclusive) >= 0) {
      throw new IllegalArgumentException(String.format("UUID %s >= %s", fromInclusive.toString(), toExclusive.toString()));
    }
    this.fromInclusive = fromInclusive;
    this.toExclusive = toExclusive;
  }

  // Java's UUID.compareTo unfortunately does the wrong thing here, treating MSB and LSB as _signed_
  // ints, so that "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF" < "11111111-1111-1111-1111-111111111111".
  private static int compare(UUID a, UUID b) {
    long cmpMSB = Long.compareUnsigned(a.getMostSignificantBits(), b.getMostSignificantBits());
    if (cmpMSB < 0) return -1;
    if (cmpMSB > 0) return 1;
    return Long.compareUnsigned(a.getLeastSignificantBits(), b.getLeastSignificantBits());
  }

  public boolean shouldInclude(UUID uuid) {
    return (fromInclusive == null || compare(fromInclusive, uuid) <= 0) &&
           (toExclusive == null || compare(toExclusive, uuid) > 0);
  }
}
