/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.signal.accountstream.accountdb;

import io.grpc.Context;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.signal.accountstream.Account;
import org.signal.accountstream.NanoDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Iterate over all records in an AccountDB. */
class AccountIterator implements Iterator<Account> {
  private static final Logger logger = LoggerFactory.getLogger(AccountIterator.class);
  private static final AtomicInteger iteratorId = new AtomicInteger();
  private static final List<Account> END_OF_SCAN_MARKER = new ArrayList<>();
  private static final Counter produceNanos = Metrics.counter("AccountIterator.produceNanos");
  private static final Counter waitForPageNanos = Metrics.counter("AccountIterator.waitForPageNanos");
  private static final Counter provideNextAccountNanos = Metrics.counter("AccountIterator.provideNextAccountNanos");
  private static final Counter pagesQueueFullWhileWriting = Metrics.counter("AccountIterator.pagesQueueFullWhileWriting");
  private static final Counter pagesQueueFullWhileReading = Metrics.counter("AccountIterator.pagesQueueFullWhileReading");
  private static final Counter pagesQueuePartialWhileWriting = Metrics.counter("AccountIterator.pagesQueuePartialWhileWriting");
  private static final Counter pagesQueuePartialWhileReading = Metrics.counter("AccountIterator.pagesQueuePartialWhileReading");
  private static final Counter pagesQueueEmptyWhileWriting = Metrics.counter("AccountIterator.pagesQueueEmptyWhileWriting");
  private static final Counter pagesQueueEmptyWhileReading = Metrics.counter("AccountIterator.pagesQueueEmptyWhileReading");

  private final AccountDB accountDB;
  // Allow a small but non-zero amount of queuing of database-supplied data within each iterator,
  // to smooth out the fact that all AccountIterators need to process data pretty much in lockstep.
  // At any time, an iterator may have a full [pages] queue as well as the currentPage set to some
  // non-null value.
  private final BlockingQueue<List<Account>> pages;
  private List<Account> currentPage = null;
  private Account nextAccount = null;
  private final int id;
  private int segmentsLeftToProcess;
  private final Context.CancellableContext context;
  private int currentPageIndex = 0;
  private final NanoDiff nanoDiff = new NanoDiff();
  private final AtomicReference<RuntimeException> error = new AtomicReference<>(null);
  private boolean first = true;

  AccountIterator(AccountDB accountDB) {
    this.accountDB = accountDB;
    // Sizing this to the total read parallelism allows all iterators to have one page queued for processing
    // and one page being requested from the remote backend.
    this.pages = new LinkedBlockingQueue<>(accountDB.getReadParallelism());

    id = iteratorId.addAndGet(1);
    logger.info("Iterator {} starting", id);
    segmentsLeftToProcess = accountDB.getTotalSegments();
    context = Context.current().withCancellation();
    nanoDiff.nanosDelta();
  }

  /** Called by SegmentReader when new data is available.
   *
   * Safe for concurrent calling by multiple SegmentReaders.
   *
   * @return true if we'd like to keep receiving data, false if we would not.
   */
  boolean acceptNextScanResponse(List<Account> response) {
    Instant start = null;
    if (pages.remainingCapacity() == 0) {
      pagesQueueFullWhileWriting.increment();
      start = Instant.now();
    } else if (pages.size() == 0) {
      pagesQueueEmptyWhileWriting.increment();
    } else {
      pagesQueuePartialWhileWriting.increment();
    }
    while (!context.isCancelled()) {
      try {
        if (pages.offer(response, 1, TimeUnit.SECONDS)) {
          if (start != null)
            logger.trace("Writing scanresponse to full iterator {} in {}", id, Duration.between(start, Instant.now()));
          return true;
        }
      } catch (InterruptedException e) {
      }
    }
    logger.trace("Iterator {} cancelled while receiving scanresponse", id);
    return false;
  }

  /** Called by SegmentReader when a segment has been read and acceptNextScanResponse called for all of it.
   *
   * Safe for concurrent calling by multiple SegmentReaders.
   */
  synchronized void segmentComplete(int segment) {
    segmentsLeftToProcess--;
    float percentComplete = (accountDB.getTotalSegments() - segmentsLeftToProcess) * 100 / (float) accountDB.getTotalSegments();
    logger.info("Iterator {} completed segment {}, {}% complete", id, segment, percentComplete);
    if (segmentsLeftToProcess == 0) {
      // Tell the consumer that they've reached the end of the line.
      acceptNextScanResponse(END_OF_SCAN_MARKER);
    }
  }

  int iteratorID() {
    return id;
  }

  void setError(RuntimeException err) {
    if (error.compareAndSet(null, err)) context.cancel(err);
    acceptNextScanResponse(END_OF_SCAN_MARKER);
  }

  private void computeFirst() {
    if (!first) return;
    first = false;
    computeNext();
  }

  private void computeNext() {
    nextAccount = null;
    while (currentPage == null || currentPage.size() <= currentPageIndex && !context.isCancelled()) {
      if (pages.remainingCapacity() == 0) {
        pagesQueueFullWhileReading.increment();
      } else if (pages.size() == 0) {
        pagesQueueEmptyWhileReading.increment();
      } else {
        pagesQueuePartialWhileReading.increment();
      }
      try {
        currentPage = pages.take();
      } catch (InterruptedException e) {
        continue;
      }
      if (currentPage == END_OF_SCAN_MARKER) {
        RuntimeException err = error.get();
        if (err != null) throw err;
        return;
      }
      currentPageIndex = 0;
      waitForPageNanos.increment(nanoDiff.nanosDelta());
    }
    if (context.isCancelled()) {
      throw Status.CANCELLED.asRuntimeException();
    }
    nextAccount = currentPage.get(currentPageIndex);
    currentPageIndex++;
    provideNextAccountNanos.increment(nanoDiff.nanosDelta());
  }

  @Override
  public boolean hasNext() {
    computeFirst();
    boolean out = nextAccount != null;
    if (!out) logger.trace("Iterator {} has returned all values to caller", id);
    return out;
  }

  @Override
  public Account next() {
    computeFirst();
    Account out = nextAccount;
    produceNanos.increment(nanoDiff.nanosDelta());
    computeNext();
    return out;
  }
}
