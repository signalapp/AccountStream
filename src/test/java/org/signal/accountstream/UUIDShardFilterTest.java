/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class UUIDShardFilterTest {
  @ParameterizedTest
  @MethodSource
  void testShouldInclude(@Nullable final UUID from, @Nullable final UUID to, final UUID uuid, final boolean expectIncluded) {
    assertEquals(expectIncluded, new UUIDShardFilter(from, to).shouldInclude(uuid));
  }

  private static Stream<Arguments> testShouldInclude() {
    final UUID from = UUID.fromString("11111111-1111-1111-1111-111111111111");
    final UUID to = UUID.fromString("22222222-2222-2222-2222-222222222222");

    return Stream.of(
        Arguments.of(from, to, UUID.fromString("00000000-1111-1111-1111-111111111111"), false),
        Arguments.of(from, to, UUID.fromString("11111111-1111-1111-1111-111111111111"), true),
        Arguments.of(from, to, UUID.fromString("22222222-1111-1111-1111-111111111111"), true),
        Arguments.of(from, to, UUID.fromString("22222222-2222-2222-2222-222222222222"), false),
        Arguments.of(from, to, UUID.fromString("ffffffff-2222-2222-2222-222222222222"), false),

        Arguments.of(null, to, UUID.fromString("00000000-1111-1111-1111-111111111111"), true),
        Arguments.of(null, to, UUID.fromString("11111111-1111-1111-1111-111111111111"), true),
        Arguments.of(null, to, UUID.fromString("22222222-1111-1111-1111-111111111111"), true),
        Arguments.of(null, to, UUID.fromString("22222222-2222-2222-2222-222222222222"), false),
        Arguments.of(null, to, UUID.fromString("ffffffff-2222-2222-2222-222222222222"), false),

        Arguments.of(from, null, UUID.fromString("00000000-1111-1111-1111-111111111111"), false),
        Arguments.of(from, null, UUID.fromString("11111111-1111-1111-1111-111111111111"), true),
        Arguments.of(from, null, UUID.fromString("22222222-1111-1111-1111-111111111111"), true),
        Arguments.of(from, null, UUID.fromString("22222222-2222-2222-2222-222222222222"), true),
        Arguments.of(from, null, UUID.fromString("ffffffff-2222-2222-2222-222222222222"), true));
  }

  @Test
  void testInvalidOrder() {
    assertThrows(IllegalArgumentException.class, () -> {
      new UUIDShardFilter(
          UUID.fromString("22222222-2222-2222-2222-222222222222"),
          UUID.fromString("11111111-1111-1111-1111-111111111111"));
    });
  }
}
