/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.signal.accountstream.accountdb;

import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.signal.accountstream.Account;
import org.signal.accountstream.NanoDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/** Reads a single segment in a parallel scan of an AccountDB, passing data to a set of AccountIterators.
 *
 * AccountDB handles database reads by creating SegmentReaders, then attaching AccountIterators to them.
 * When a SegmentReader is run, it's removed from the AccountDB list of scheduled readers, at which point
 * no future AccountIterators are added to it.  It then runs, reading in its database segment and passing
 * the data to all iterators that attached to it before scheduling.
 */
class SegmentReader implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(SegmentReader.class);
  private static final Counter requestingPageNanos = Metrics.counter("SegmentReader.requestingPageNanos");
  private static final Counter providingPageNanos = Metrics.counter("SegmentReader.providingPageNanos");
  private static final Counter decodeAccountNanos = Metrics.counter("SegmentReader.decodeAccountNanos");

  private final AccountDB accountDB;
  private int segment;
  private Set<AccountIterator> iterators;
  private final NanoDiff nanoDiff = new NanoDiff();

  SegmentReader(AccountDB accountDB, int segment, AccountIterator first) {
    this.accountDB = accountDB;
    this.segment = segment;
    this.iterators = new HashSet<>();
    this.iterators.add(first);
  }

  @Override
  public void run() {
    accountDB.removeSegment(segment);
    // After this point, SegmentReader is no longer referenced by its top-level AccountDB,
    // so we can assume that this.iterators will be modified only within the confines of this function,
    // without further locking.
    logger.trace("Starting to read segment {} for iterators {}", segment, iterators.stream().map(s -> s.iteratorID()).toList());

    nanoDiff.nanosDelta();
    try {
      // Read all data from our segment, ...
      for (ScanResponse response : accountDB.getClient().scanPaginator(
          ScanRequest.builder()
              .segment(segment)
              .totalSegments(accountDB.getTotalSegments())
              .tableName(accountDB.getAccountTableName())
              .attributesToGet(
                  AccountDB.KEY_ACCOUNT_UUID,
                  AccountDB.ATTR_ACCOUNT_E164,
                  AccountDB.ATTR_CANONICALLY_DISCOVERABLE,
                  AccountDB.ATTR_PNI,
                  AccountDB.ATTR_UAK)
              .build())) {
        requestingPageNanos.increment(nanoDiff.nanosDelta());
        // ... decode them, ...
        List<Account> accounts = new ArrayList<>(response.items().size());
        for (Map<String, AttributeValue> item : response.items()) {
          accounts.add(AccountDB.accountFromItem(item));
        }
        decodeAccountNanos.increment(nanoDiff.nanosDelta());
        // ... and pass it to each iterator that wants it ...
        for (Iterator<AccountIterator> it = iterators.iterator(); it.hasNext(); ) {
          AccountIterator element = it.next();
          // ... removing any iterators that no longer need the data ...
          if (!element.acceptNextScanResponse(accounts)) it.remove();
        }
        // ... and cancelling completely if all iterators say they're done.
        if (iterators.isEmpty()) {
          logger.trace("Segment {} no remaining iterators", segment);
          return;
        }
        providingPageNanos.increment(nanoDiff.nanosDelta());
      }
      // Tell any iterators that didn't cancel early that this segment has been successfully and fully written.
      for (AccountIterator iter : iterators) {
        iter.segmentComplete(segment);
      }
      logger.debug("Segment {} complete", segment);
    } catch (Exception e) {
      logger.error("Error reading segment {}", segment, e);
      RuntimeException re = Status.INTERNAL.withDescription("read failed for segment " + segment).withCause(e).asRuntimeException();
      for (AccountIterator iter : iterators) {
        iter.setError(re);
      }
      iterators.clear();
    }
  }

  void addIterator(AccountIterator iter) {
    iterators.add(iter);
  }
}
