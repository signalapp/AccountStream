/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream.accountdb;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.signal.accountstream.Account;
import org.signal.accountstream.UUIDUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** AccountDB allows for efficient reading of a large database by multiple readers.
 *
 * AccountDB utilizes DynamoDB's parallel scanning to read multiple streams from a single
 * database at once, allowing for parallel upload of that data to its clients.  It does so, though,
 * in a way that allows for arbitrary numbers of clients to all receive the same data, even
 * if clients start their requests at different times.  It utilizes the following algorithm to do this:
 *
 *   - an executor, segmentReadExecutor, is created with R read threads
 *   - all reads are split into S segments
 *   - a caller requests a stream of account data:
 *     - the caller creates SegmentReader requests sr1...srS, one for each segment
 *     - segmentReadExecutor executes these, with parallelism R
 *   - some number A of these requests finish, and R are currently in progress
 *   - a second caller requests a stream of account data
 *     - the caller adds themselves as a recipient to all unstarted SegmentReader request
 *     - the caller creates new SegmentReader requests for any it wasn't able to add itself to
 *     - when segmentReadExecutor executes shared requests, it now passes the data to the first and second caller
 */
@Singleton
public class AccountDB {
  private static final Logger logger = LoggerFactory.getLogger(AccountDB.class);

  @VisibleForTesting public static final String KEY_ACCOUNT_UUID = "U";
  @VisibleForTesting public static final String ATTR_ACCOUNT_E164 = "P";
  @VisibleForTesting public static final String ATTR_CANONICALLY_DISCOVERABLE = "C";
  @VisibleForTesting public static final String ATTR_PNI = "PNI";
  @VisibleForTesting public static final String ATTR_UAK = "UAK";

  @VisibleForTesting
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  private final DynamoDbClient client;
  private final String accountTableName;
  private final int totalSegments;
  private final Executor segmentReadExecutor;
  private final int readParallelism;
  private Map<Integer, SegmentReader> segmentReaders = new HashMap<>();

  public AccountDB(
      final DynamoDbClient client,
      @Value("${accountdb.tablename}") String accountTableName,
      @Value("${accountdb.segments:1000}") int segments,
      @Value("${accountdb.read-parallelism:8}") int readParallelism) {
    this.client = client;
    this.accountTableName = accountTableName;
    this.totalSegments = segments;
    this.readParallelism = readParallelism;
    this.segmentReadExecutor = Executors.newFixedThreadPool(readParallelism);
  }

  int getTotalSegments() {
    return totalSegments;
  }

  int getReadParallelism() {
    return readParallelism;
  }

  public Iterable<Account> listAccounts() {
    return new Iterable<Account>() {
      @Override
      public Iterator<Account> iterator() {
        // We know we want to read all segments [0, totalSegments).  But to make sure that we're reading
        // uniformly from the entire database, we shuffle the order in which we read them.
        List<Integer> shuffledSegments = new ArrayList<>(totalSegments);
        for (int i = 0; i < totalSegments; i++) {
          shuffledSegments.add(i);
        }
        Collections.shuffle(shuffledSegments);

        AccountIterator iter = new AccountIterator(AccountDB.this);
        for (int segment : shuffledSegments) {
          // In cases where AccountDB already has a scheduled-but-not-yet-running read of a segment,
          // we'll attach on to that and read from it instead of scheduling a new one.  Otherwise, we'll schedule
          // a new read of the segment and subscribe to it.
          requestSegment(segment, iter);
        }
        return iter;
      }
    };
  }

  // End of public API

  String getAccountTableName() {
    return accountTableName;
  }

  DynamoDbClient getClient() {
    return client;
  }

  /** request that database segment [segment] be read and its data supplied to AccountIterator [iter].
   *
   * This will make sure that a read of [segment] is scheduled.  If none is, a new SegmentReader will be
   * created to schedule it.  If one is scheduled, [iter] is added to the existing SegmentReader.
   * In this way, scheduled-but-not-yet-running SegmentReaders gather all the AccountIterators that
   * want their data, then when run are able to share it down to all of them.
   *
   * @param segment Database segment, in range [0, totalSegments)
   * @param iter  Iterator that needs this segment passed to it.
   */
  synchronized void requestSegment(int segment, AccountIterator iter) {
    SegmentReader reader = segmentReaders.get(segment);
    if (reader == null) {
      logger.trace("New segment {} scheduled for iterator {}", segment, iter.iteratorID());
      reader = new SegmentReader(this, segment, iter);
      segmentReaders.put(segment, reader);
      segmentReadExecutor.execute(reader);
    } else {
      reader.addIterator(iter);
    }
  }

  /** Removes the given [segment] from the list of scheduled-but-not-yet-run SegmentReaders, generally when started. */
  synchronized void removeSegment(int segment) {
    segmentReaders.remove(segment);
  }

  static Account accountFromItem(Map<String, AttributeValue> item) throws IOException {
    UUID uuid = UUIDUtil.uuidFromByteBuffer(item.get(AccountDB.KEY_ACCOUNT_UUID).b().asByteBuffer());
    long e164 = e164FromString(item.get(AccountDB.ATTR_ACCOUNT_E164).s());
    boolean canonicallyDiscoverable = Optional.ofNullable(item.get(ATTR_CANONICALLY_DISCOVERABLE)).map(x -> x.bool()).orElse(false);
    final UUID pni;
    if (item.containsKey(ATTR_PNI) && item.get(ATTR_PNI).b() != null) {
      pni = UUIDUtil.uuidFromByteBuffer(item.get(ATTR_PNI).b().asByteBuffer());
    } else {
      pni = null;
    }
    final byte[] uak;
    if (item.containsKey(ATTR_UAK) && item.get(ATTR_UAK).b() != null) {
      uak = item.get(ATTR_UAK).b().asByteArray();
    } else {
      uak = null;
    }
    return new Account(e164, uuid, canonicallyDiscoverable, pni, uak);
  }

  private static long e164FromString(String s) {
    Preconditions.checkNotNull(s);
    if (!s.startsWith("+")) {
      throw new IllegalArgumentException("e164 not prefixed with '+'");
    }
    return Long.parseLong(s.substring(1));
  }
}
