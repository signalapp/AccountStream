/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

public class NanoDiff {
  private AtomicReference<Instant> last = new AtomicReference<>(Instant.now());
  public long nanosDelta() {
    Instant now = Instant.now();
    Instant last = this.last.getAndSet(now);
    return Duration.between(last, now).toNanos();
  }
}
