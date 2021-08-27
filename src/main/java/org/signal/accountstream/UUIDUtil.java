/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.UUID;

public class UUIDUtil {
  public static UUID uuidFromByteString(ByteString bs) {
    if (bs.isEmpty()) return null;
    return uuidFromByteBuffer(bs.asReadOnlyByteBuffer());
  }

  public static UUID uuidFromByteBuffer(ByteBuffer bb) {
    long msb = bb.getLong();
    long lsb = bb.getLong();
    return new UUID(msb, lsb);
  }

  public static ByteBuffer byteBufferFromUUID(UUID uuid) {
    return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).flip();
  }
}
