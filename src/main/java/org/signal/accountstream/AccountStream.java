/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import static org.signal.accountstream.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.micrometer.core.instrument.Metrics;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.TaskExecutors;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.signal.accountstream.proto.ContinuationTokenInternal;
import org.signal.accountstream.stream.ShardHorizon;
import org.signal.accountstream.stream.ShardSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

@Singleton
public class AccountStream {
  private static final Logger logger = LoggerFactory.getLogger(AccountStream.class);
  private static final Duration REREAD_SHARDSET_EVERY = Duration.ofMinutes(5);

  private static final String DESCRIBE_STREAM_COUNTER_NAME = name(AccountStream.class, "describeStream");
  private static final String GET_SHARD_ITERATOR_COUNTER_NAME = name(AccountStream.class, "getShardIterator");
  private static final String GET_RECORDS_COUNTER_NAME = name(AccountStream.class, "getRecords");

  private final KinesisAsyncClient stream;
  private final Executor executor;
  private final String streamName;
  private ShardSet shardSet = null;
  private Instant shardSetRead = null;
  private final LRUCache<ContinuationTokenInternal, CompletableFuture<AccountUpdateStreamPage>> cache = new LRUCache<>(64);

  public AccountStream(
      KinesisAsyncClient stream,
      @Value("${accountdb.streamname}") String streamName,
      @Named(TaskExecutors.IO) Executor executor) {
    this.stream = stream;
    this.streamName = streamName;
    this.executor = executor;
  }

  @VisibleForTesting
  synchronized void clearCache() {
    cache.clear();
  }

  public record AccountUpdateStreamPage(
    List<Account> updates,
    ContinuationTokenInternal continuationToken,
    boolean caughtUpToRealtime) {

    public AccountUpdateStreamPage filter(UUIDShardFilter filter) {
      return new AccountUpdateStreamPage(
          updates.stream().filter(a -> filter.shouldInclude(a.uuid)).toList(),
          continuationToken,
          caughtUpToRealtime);
    }
  }

  public ContinuationTokenInternal beginningOfStreamContinuationToken() throws StatusException {
    return latestShardSet().fromTheBeginning().toContinuationToken();
  }

  public boolean checkContinuationTokenStillValid(ContinuationTokenInternal continuationToken) throws StatusException {
    if (continuationToken.getShardHorizonSequenceNumbersCount() == 0) {
      logger.debug("Continuation token empty, reporting as invalid");
      return false;
    }
    ShardHorizon horizon = ShardHorizon.fromContinuationToken(continuationToken);
    if (!latestShardSet().validHorizon(horizon)) {
      logger.debug("Shard horizon not valid in set, reporting as invalid");
      return false;
    }
    logger.debug("Shard horizon valid");
    return true;
  }

  /**
   * Convert an exception to the appropriate gRPC status
   *
   */
  private static Status marshalException(final Exception e) {
    if ((e instanceof ExecutionException ee) && (ee.getCause() instanceof ProvisionedThroughputExceededException ex)) {
      // exception indicates rate limiting, the caller may want to
      // retry this
      return Status.fromCode(Code.RESOURCE_EXHAUSTED).withCause(ex);
    }
    return Status.fromCode(Code.INTERNAL).withCause(e);
  }

  /**
   * Return the next set of dynamo records from the given client with the given shards, updating our internal state to
   * point after those records.
   *
   * Upon a successful call to nextRecords, toContinuationToken() is updated such that a ShardHorizon created with
   * fromContinuationToken() will not provide the returned records again.  nextRecords() may return no records.  If no
   * records are returned, this horizon is at realtime and no new records are available.
   */
  public AccountUpdateStreamPage streamNextUpdates(ContinuationTokenInternal continuationToken) throws StatusException {
    final CompletableFuture<AccountUpdateStreamPage> out;
    boolean fromPrevious = false;
    synchronized (this) {
      if (cache.containsKey(continuationToken)) {
        out = cache.get(continuationToken);
        fromPrevious = true;
      } else {
        out = new CompletableFuture<>();
        cache.put(continuationToken, out);
      }
    }
    if (fromPrevious) {
      logger.trace("Returning cached update page");
      try {
        return cache.get(continuationToken).get();
      } catch (Exception e) {
        throw Status.fromThrowable(e).asException();
      }
    }
    try {
    ShardHorizon horizon = ShardHorizon.fromContinuationToken(continuationToken);
      logger.trace("Streaming from {}", horizon);
      ShardSet set = latestShardSet();
      if (!set.validHorizon(horizon)) {
        throw Status.fromCode(Code.DATA_LOSS).withDescription("horizon no longer valid, updates skipped").asException();
      }

      Map<String, CompletableFuture<GetRecordsResponse>> recordsRequests = horizonRecords(horizon);

      List<Account> output = new ArrayList<>();
      boolean caughtUpToRealtime = true;
      for (Map.Entry<String, CompletableFuture<GetRecordsResponse>> entry : recordsRequests.entrySet()) {
        final GetRecordsResponse resp;
        try {
          resp = entry.getValue().get();
        } catch (Exception e) {
          throw marshalException(e).withDescription("getting shard horizon records").asException();
        }
        if (resp.millisBehindLatest() == null || resp.millisBehindLatest() > 0) {
          caughtUpToRealtime = false;
        }
        if (resp.hasRecords() && !resp.records().isEmpty()) {
          String lastProvidedSequence = "";
          for (Record record : resp.records()) {
            lastProvidedSequence = record.sequenceNumber();
            try {
              output.add(Account.fromInputStream(record.data().asInputStream()));
            } catch (IOException e) {
              throw Status.fromCode(Code.INTERNAL).withDescription("parsing account from record").withCause(e)
                  .asException();
            }
          }
          logger.trace("Marking horizon key '{}' with sequence '{}'", entry.getKey(), lastProvidedSequence);
          horizon.markShardSequenceComplete(entry.getKey(), lastProvidedSequence, set);
        }
        if (resp.nextShardIterator() == null) {
          logger.trace("Marking horizon key '{}' closed", entry.getKey());
          horizon.markShardSequenceClosed(entry.getKey(), set);
        }
      }
      logger.trace("Returning {} records", output.size());
      if (horizon.toContinuationToken().equals(continuationToken)) {
        // No progress has been made, since the continuation tokens are the same.  Remove this from cache.
        synchronized (this) {
          cache.remove(continuationToken);
        }
      }
      out.complete(new AccountUpdateStreamPage(output, horizon.toContinuationToken(), caughtUpToRealtime));
    } catch (Exception e) {
      synchronized (this) {
        cache.remove(continuationToken);
      }
      out.completeExceptionally(e);
    }
    try {
      return out.get();
    } catch (Exception e) {
      throw Status.fromThrowable(e).asException();
    }
  }

  // End of public API

  private synchronized ShardSet latestShardSet() throws StatusException {
    Instant expiration = Instant.now().minus(REREAD_SHARDSET_EVERY);
    if (shardSet == null || shardSetRead == null || shardSetRead.isBefore(expiration)) {
      logger.trace("Refreshing shard set");
      shardSetRead = Instant.now();
      final List<Shard> shards = new ArrayList<>();
      DescribeStreamRequest req = DescribeStreamRequest.builder()
          .streamName(streamName)
          .build();
      final String lastShardId[] = new String[]{""};
      while (true) {
        final DescribeStreamResponse resp;
        try {
          Metrics.counter(DESCRIBE_STREAM_COUNTER_NAME).increment();
          resp = stream.describeStream(req).get();
        } catch (Exception e) {
          // Failure might be retryable, but just let the
          // caller retry the whole operation if they want
          throw marshalException(e).withDescription("describeStream call to AWS failed").asException();
        }
        resp.streamDescription().shards().stream().forEach(shard -> {
          shards.add(shard);
          lastShardId[0] = shard.shardId();
        });
        if (!Optional.ofNullable(resp.streamDescription().hasMoreShards()).orElse(false)) {
          logger.trace("Have all stream description data");
          break;
        }
        logger.trace("Requesting next stream description data");
        req = req.toBuilder().exclusiveStartShardId(lastShardId[0]).build();
      }
      shardSet = new ShardSet(shards);
    }
    return shardSet;
  }

  /**
   * Generate a new iterator for each shard currently on the horizon, and get the first set of records from each of
   * those iterators.
   */
  private Map<String, CompletableFuture<GetRecordsResponse>> horizonRecords(ShardHorizon horizon) throws StatusException {
    logger.trace("Getting next set of horizon records");
    Map<String, CompletableFuture<GetRecordsResponse>> recordsRequests = new HashMap<>();
    for (Map.Entry<String, String> entry : horizon.getShardIDsToCompletedSequenceNumbers().entrySet()) {
      GetShardIteratorRequest.Builder shardIterRequest = GetShardIteratorRequest.builder()
          .streamName(streamName)
          .shardId(entry.getKey());
      if (ShardHorizon.SHARD_NOT_STARTED.equals(entry.getValue())) {
        shardIterRequest.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
      } else {
        shardIterRequest.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER).startingSequenceNumber(entry.getValue());
      }
      Metrics.counter(GET_SHARD_ITERATOR_COUNTER_NAME).increment();
      recordsRequests.put(entry.getKey(),
          stream.getShardIterator(shardIterRequest.build())
              .thenComposeAsync(
                  shardIterResp -> getRecordsFromShardIterator(entry.getKey(), shardIterResp.shardIterator()),
                  executor));
    }
    return recordsRequests;
  }

  /** Iterate over a single shard until we reach the end or some records.
   *
   * A Kinesis shard iterator may return no records (repeatably, forever) on the first call to getRecords(iter).
   * Kinesis breaks up shards (it appears) into time regions, and if we haven't seen any records in the first
   * hour of a shard, getRecords(getIter(shardID)) will always return "there's none here".  We want to get the
   * first set of records (or reach the end of the shard iterator), so this function loops over a shard iterator
   * and the subsequent "next iterators for shard" that each returns, until it finds the end or some records.
   */
  private CompletableFuture<GetRecordsResponse> getRecordsFromShardIterator(String shardID, String shardIterator) {
    Metrics.counter(GET_RECORDS_COUNTER_NAME).increment();
    return stream.getRecords(GetRecordsRequest.builder()
        .shardIterator(shardIterator)
        .build())
        .thenComposeAsync(response -> {
          int numRecords = response.hasRecords() ? response.records().size() : 0;
          boolean hasNextShard = !Optional.ofNullable(response.nextShardIterator()).orElse("").isEmpty()
              && response.millisBehindLatest() > 0;
          if (numRecords > 0 || !hasNextShard) {
            logger.trace("Shard {} records={} iter={} (behind={}ms)", shardID, numRecords, hasNextShard, response.millisBehindLatest());
            return CompletableFuture.completedFuture(response);
          } else {
            logger.trace("Shard {} no records and has next, requesting next.  millisBehind={}", shardID, response.millisBehindLatest());
            return getRecordsFromShardIterator(shardID, response.nextShardIterator());
          }
        }, executor);
  }
}
