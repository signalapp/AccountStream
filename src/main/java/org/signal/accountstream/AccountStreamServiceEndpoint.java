/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import static org.signal.accountstream.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.signal.accountstream.AccountStream.AccountUpdateStreamPage;
import org.signal.accountstream.accountdb.AccountDB;
import org.signal.accountstream.proto.AccountDataRequest;
import org.signal.accountstream.proto.AccountDataStream;
import org.signal.accountstream.proto.AccountDataStream.Record;
import org.signal.accountstream.proto.AccountStreamServiceGrpc.AccountStreamServiceImplBase;
import org.signal.accountstream.proto.ContinuationTokenInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class AccountStreamServiceEndpoint extends AccountStreamServiceImplBase {
  private static final Logger logger = LoggerFactory.getLogger(AccountStreamServiceEndpoint.class);
  private static final int MAX_RECORDS_PER_STREAM_MESSAGE = 4000;
  private static final Counter dbReceiveNanos = Metrics.counter("AccountStreamServiceEndpoint.dbReceiveNanos");
  private static final Counter dbFilterNanos = Metrics.counter("AccountStreamServiceEndpoint.dbFilterNanos");
  private static final Counter dbAddToStreamNanos = Metrics.counter("AccountStreamServiceEndpoint.dbAddToStreamNanos");
  private static final Counter dbSendNanos = Metrics.counter("AccountStreamServiceEndpoint.dbSendNanos");

  private final Timer fullDynamoSnapshotPullTimer;
  private static final String ERROR_COUNTER_NAME = name(AccountStreamServiceEndpoint.class, "errors");
  private static final Counter RATE_LIMIT_COUNTER = Metrics.counter(name(AccountStreamServiceEndpoint.class, "rateLimited"));

  private final AccountDB accountDB;
  private final AccountStream accountStream;
  private final long maxSleepMillis;
  private final long minSleepMillis;

  public AccountStreamServiceEndpoint(
      AccountDB accountDB,
      AccountStream accountStream,
      MeterRegistry meterRegistry,
      @Value("${streaming.min-sleep:PT0.25S}") Duration minSleep,
      @Value("${streaming.max-sleep:PT1M}") Duration maxSleep) {
    Preconditions.checkArgument(minSleep.compareTo(maxSleep) < 0);
    this.accountDB = accountDB;
    this.accountStream = accountStream;
    this.maxSleepMillis = maxSleep.toMillis();
    this.minSleepMillis = minSleep.toMillis();

    // not created statically because common tags are only set during bean creation
    this.fullDynamoSnapshotPullTimer = Timer.builder(name(AccountStreamServiceEndpoint.class, "dynamoPulls"))
        .register(meterRegistry);
  }

  private static String logreq(AccountDataRequest req) {
    return String.format("request[%s,%s]", UUIDUtil.uuidFromByteString(req.getShard().getStart()), UUIDUtil.uuidFromByteString(req.getShard().getLimit()));
  }

  // This response stream will, if all goes well, never complete.  So we don't want
  // to wait around for its completion within this call, which is taking up a micronaut
  // serving thread.  Rather, we use the unbounded IO executor.
  @ExecuteOn(TaskExecutors.IO)
  @Override
  public void streamAccountData(AccountDataRequest request,
      io.grpc.stub.StreamObserver<AccountDataStream> responseObserver) {
    logger.info("Received new streaming request {}", logreq(request));
    Context ctx = Context.current();
    boolean errorReturned = false;
    try {
      UUID from = UUIDUtil.uuidFromByteString(request.getShard().getStart());
      UUID to = UUIDUtil.uuidFromByteString(request.getShard().getLimit());
      UUIDShardFilter filter = new UUIDShardFilter(from, to);

      ContinuationTokenInternal continuationToken;
      try {
        continuationToken = ContinuationTokenInternal.parseFrom(request.getLastContinuationToken());
      } catch (InvalidProtocolBufferException e) {
        throw Status.fromCode(Code.INVALID_ARGUMENT).withCause(e).asException();
      }
      logger.info("Streaming updates");
      if (accountStream.checkContinuationTokenStillValid(continuationToken)) {
        logger.info("Received valid continuation token");
      } else {
        logger.info("Streaming initial snapshot");
        pushFullSnapshotFromDynamo(responseObserver, filter, ctx);
        // our continuation token was no longer valid,
        // we'll fetch a new one when we start consuming the stream
        continuationToken = null;
      }
      streamUpdates(responseObserver, Optional.ofNullable(continuationToken), filter, ctx);
    } catch (Throwable t) {
      logger.warn("Streaming stopped for {}", logreq(request), t);
      responseObserver.onError(t);
      errorReturned = true;
    } finally {
      if (!errorReturned) {
        logger.warn("Streaming cancelled for {}", logreq(request));
        responseObserver.onError(Status.fromCode(Code.CANCELLED).withDescription("streaming cancellation detected").asException());
      }
      Metrics.counter(ERROR_COUNTER_NAME,
          "errorReturned", String.valueOf(errorReturned))
          .increment();
    }
  }

  /** Returns newValue squashed between min and max. */
  private static long updateInRange(long newValue, long min, long max) {
    return Math.max(min, Math.min(max, newValue));
  }

  /** streamUpdates streams account updates down to the given client.  Should it be unable to continue to do
   * so, because the stream is out of date, it returns.  Otherwise, blocks forever.
   */
  private void streamUpdates(StreamObserver<AccountDataStream> responseObserver,
      Optional<ContinuationTokenInternal> continuationToken, UUIDShardFilter filter, final Context ctx)
      throws StatusException {
    long sleepMillis = 0;
    boolean completed = false;

    while (!ctx.isCancelled()) {
      try {
        logger.trace("Sleeping for {}ms", sleepMillis);
        Thread.sleep(sleepMillis);
      } catch (InterruptedException e) {
        throw Status.INTERNAL.withDescription("interrupted stream sleep").withCause(e).asException();
      }

      final AccountUpdateStreamPage nextPage;
      try {
        if (continuationToken.isEmpty()) {
          // Get the initial continuation token. This may have to look up the stream
          // which may be subject to rate limits
          continuationToken = Optional.of(accountStream.beginningOfStreamContinuationToken());
        }
        nextPage = accountStream.streamNextUpdates(continuationToken.get()).filter(filter);
      } catch (StatusException e) {
        if (e.getStatus().getCode() != Code.RESOURCE_EXHAUSTED) { throw e; }

        // We failed due to hitting a rate limit, sleep longer and try again
        sleepMillis = updateInRange(sleepMillis*2, minSleepMillis, maxSleepMillis);
        RATE_LIMIT_COUNTER.increment();
        logger.debug("Stream rate limited, new sleep time: {}", sleepMillis);
        continue;
      }

      Preconditions.checkState(continuationToken.isPresent());

      // The continuation token changes when we've moved forward substantially within the stream of data
      // coming from our underlying update stream.  This will always occur when new updates are available,
      // but it may also occur when they're not.  A good example of this is if one (empty) shard splits
      // into two (empty) shards.  In that case, there are no updates, but the shard iterator should move
      // from (old1) to (new1,new2).
      if (continuationToken.get().equals(nextPage.continuationToken())) {
        Preconditions.checkState(nextPage.updates().isEmpty(),
            "updates nonempty but continuation token didn't change");
        if (!completed) {
          responseObserver.onNext(AccountDataStream.newBuilder().setDatasetCurrent(true).build());
          completed = true;
        }
        sleepMillis = updateInRange(sleepMillis*2, minSleepMillis, maxSleepMillis);
      } else {
        continuationToken = Optional.of(nextPage.continuationToken());
        completed = completed || nextPage.caughtUpToRealtime();
        nextPageToStream(nextPage, responseObserver, completed, MAX_RECORDS_PER_STREAM_MESSAGE);
        sleepMillis = updateInRange(sleepMillis/2, minSleepMillis, maxSleepMillis);
      }
    }
  }

  @VisibleForTesting
  static void nextPageToStream(
      AccountUpdateStreamPage nextPage,
      StreamObserver<AccountDataStream> responseObserver,
      boolean completed,
      int maxRecordsPerMessage) {
    AccountDataStream.Builder page = AccountDataStream.newBuilder();
    for (Account account : nextPage.updates()) {
      addAccountToDataStream(page, account);
      if (page.getRecordsCount() >= maxRecordsPerMessage) {
        logger.trace("Sending full intermediate page with {}", page.getRecordsCount());
        responseObserver.onNext(page.build());
        page = AccountDataStream.newBuilder();
      }
    }
    page.setDatasetCurrent(completed);
    page.setContinuationToken(nextPage.continuationToken().toByteString());
    logger.trace("Sending final page with {}, complete={}", page.getRecordsCount(), completed);
    responseObserver.onNext(page.build());
  }

  private static void addAccountToDataStream(AccountDataStream.Builder builder, Account account) {
    Record.Builder record = Record.newBuilder()
        .setE164(account.e164);
    if (account.canonicallyDiscoverable) {
      record.setAci(ByteString.copyFrom(UUIDUtil.byteBufferFromUUID(account.uuid)));
    }
    if (account.pni != null) {
      record.setPni(ByteString.copyFrom(UUIDUtil.byteBufferFromUUID(account.pni)));
    }
    if (account.uak != null) {
      record.setUak(ByteString.copyFrom(account.uak));
    }
    builder.addRecords(record);
  }

  private void pushFullSnapshotFromDynamo(StreamObserver<AccountDataStream> responseObserver, UUIDShardFilter filter,
      final Context ctx) {
    responseObserver.onNext(AccountDataStream.newBuilder().setClearAll(true).build());
    AtomicLong pushCount = new AtomicLong();
    AtomicLong totalCount = new AtomicLong();
    fullDynamoSnapshotPullTimer.record(() -> {
      AccountDataStream.Builder page = AccountDataStream.newBuilder();
      NanoDiff nanoDiff = new NanoDiff();
      for (Account account : accountDB.listAccounts()) {
        dbReceiveNanos.increment(nanoDiff.nanosDelta());
        totalCount.incrementAndGet();
        if (ctx.isCancelled()) {
          return;
        }
        // Because we cleared all data above, we don't need to send delete records for e164s not to be shared.
        if (!account.canonicallyDiscoverable || !filter.shouldInclude(account.uuid)) {
          dbFilterNanos.increment(nanoDiff.nanosDelta());
          continue;
        }
        dbFilterNanos.increment(nanoDiff.nanosDelta());
        addAccountToDataStream(page, account);
        dbAddToStreamNanos.increment(nanoDiff.nanosDelta());
        if (page.getRecordsCount() >= MAX_RECORDS_PER_STREAM_MESSAGE) {
          logger.trace("Pushing {} during snapshot, {}/{} pushed", page.getRecordsCount(), pushCount.addAndGet(page.getRecordsCount()), totalCount.get());
          responseObserver.onNext(page.build());
          page = AccountDataStream.newBuilder();
          dbSendNanos.increment(nanoDiff.nanosDelta());
        }
      }

      if (page.getRecordsCount() > 0) {
        logger.trace("Pushing {} at end of snapshot, {}/{} pushed", page.getRecordsCount(), pushCount.addAndGet(page.getRecordsCount()), totalCount.get());
        responseObserver.onNext(page.build());
        dbSendNanos.increment(nanoDiff.nanosDelta());
      }
    });
  }
}
