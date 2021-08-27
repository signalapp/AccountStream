/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import io.micronaut.grpc.annotation.GrpcChannel;
import io.micronaut.grpc.server.GrpcServerChannel;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.accountstream.accountdb.AccountDB;
import org.signal.accountstream.proto.AccountDataRequest;
import org.signal.accountstream.proto.AccountDataStream;
import org.signal.accountstream.proto.AccountDataStream.Record;
import org.signal.accountstream.proto.AccountStreamServiceGrpc;
import org.signal.accountstream.proto.AccountStreamServiceGrpc.AccountStreamServiceStub;
import org.signal.accountstream.proto.ContinuationTokenInternal;
import org.signal.accountstream.proto.Shard;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

@MicronautTest
class AccountStreamTest {

  @Factory
  static class AccountStreamServiceTestClientFactory {
    @Bean
    AccountStreamServiceStub stub(
        @GrpcChannel(GrpcServerChannel.NAME) final ManagedChannel channel) {
      return AccountStreamServiceGrpc.newStub(channel);
    }
  }

  @Inject
  AccountStreamServiceGrpc.AccountStreamServiceStub stub;

  private static final String ACCOUNTS_TABLE_NAME = "accounts";

  @MockBean(AccountDB.class)
  AccountDB accountDB() {
    return mock(AccountDB.class);
  }

  @MockBean(KinesisAsyncClient.class)
  KinesisAsyncClient kinesisAsyncClient() {
    return mock(KinesisAsyncClient.class);
  }

  @Inject
  KinesisAsyncClient kinesis;
  @Inject
  AccountStream accountStream;
  @Inject
  AccountDB accountDB;

  @Value("${accountdb.streamname}")
  String streamName;

  private static class StreamCollector implements StreamObserver<AccountDataStream> {
    private final List<AccountDataStream> got = new ArrayList<>();
    CompletableFuture<List<AccountDataStream>> results = new CompletableFuture<>();
    CompletableFuture<Void> completed = new CompletableFuture<>();

    @Override
    public void onNext(final AccountDataStream accountDataStream) {
      got.add(accountDataStream);
      if (accountDataStream.getDatasetCurrent()) results.complete(got);
    }

    @Override
    public void onError(final Throwable throwable) {
      results.completeExceptionally(throwable);
      completed.completeExceptionally(throwable);
    }

    @Override
    public void onCompleted() {
      results.completeExceptionally(new RuntimeException("did not expect stream to complete"));
      completed.complete(null);
    }
  }

  /**
   * Return a pair of record returnable by kinesis getRecords and the resulting Record returned by the gRPC call
   */
  private static Pair<software.amazon.awssdk.services.kinesis.model.Record, Record> record(final String seqno,
      long ei64, final UUID uuid, final UUID pni, final byte[] uak) throws JsonProcessingException {
    return Pair.of(
        software.amazon.awssdk.services.kinesis.model.Record.builder()
            .sequenceNumber(seqno)
            .data(SdkBytes.fromByteArray(AccountDB.OBJECT_MAPPER.writeValueAsBytes(
                new Account(ei64, uuid, false, pni, uak))))
            .build(),
        Record.newBuilder()
            .setE164(ei64)
            .setPni(pni == null ? ByteString.EMPTY : ByteString.copyFrom(UUIDUtil.byteBufferFromUUID(pni)))
            .setUak(uak == null ? ByteString.EMPTY : ByteString.copyFrom(uak))
            .build());
  }

  @BeforeEach
  void clearAccountStreamCache() {
    accountStream.clearCache();
  }

  @AfterEach
  void after() {
    reset(accountDB);
    reset(kinesis);
  }

  @Test
  void testBasic() throws Throwable {
    when(accountDB.listAccounts()).thenReturn(List.of());

    final Pair<software.amazon.awssdk.services.kinesis.model.Record, Record> record1 = record("0000", 11,
        UUID.fromString("11111111-1111-1111-1111-111111111111"),
        UUID.fromString("11111111-1111-1111-1111-222222222222"), new byte[16]);

    // will be filtered out
    final Pair<software.amazon.awssdk.services.kinesis.model.Record, Record> record2 = record("0001", 22,
        UUID.fromString("22111111-1111-1111-1111-111111111111"), null, null);

    when(kinesis.describeStream((DescribeStreamRequest) any())).thenReturn(CompletableFuture.completedFuture(
        DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder()
                .shards(software.amazon.awssdk.services.kinesis.model.Shard.builder()
                    .shardId("shard1")
                    .sequenceNumberRange(SequenceNumberRange.builder()
                        .startingSequenceNumber("0000")
                        .build())
                    .build())
                .build())
            .build()));
    when(kinesis.getShardIterator(GetShardIteratorRequest.builder()
        .streamName(streamName)
        .shardId("shard1")
        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
        .build())).thenReturn(CompletableFuture.completedFuture(GetShardIteratorResponse.builder()
        .shardIterator("sharditer1")
        .build()));
    when(kinesis.getRecords(GetRecordsRequest.builder()
        .shardIterator("sharditer1")
        .build())).thenReturn(CompletableFuture.completedFuture(GetRecordsResponse.builder()
        .records(record1.getLeft(), record2.getLeft())
        .build()));
    when(kinesis.getShardIterator(GetShardIteratorRequest.builder()
            .streamName(streamName)
            .shardId("shard1")
            .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
            .startingSequenceNumber("0001")
        .build())).thenReturn(CompletableFuture.completedFuture(GetShardIteratorResponse.builder()
        .shardIterator("sharditer2")
        .build()),
        new CompletableFuture<GetShardIteratorResponse>());
    when(kinesis.getRecords(GetRecordsRequest.builder()
        .shardIterator("sharditer2")
        .build())).thenReturn(CompletableFuture.completedFuture(GetRecordsResponse.builder()
        .records(Collections.EMPTY_LIST)
        .build()));

    StreamCollector sc = new StreamCollector();
    stub.streamAccountData(AccountDataRequest.newBuilder()
            .setShard(Shard.newBuilder()
                .setLimit(ByteString.copyFrom(UUIDUtil.byteBufferFromUUID(UUID.fromString("20000000-0000-0000-0000-000000000000"))))
                .build())
            .build(), sc);
    final List<AccountDataStream> got = sc.results.get();
    assertEquals(3, got.size());
    assertEquals(AccountDataStream.newBuilder().setClearAll(true).build(), got.get(0));
    assertEquals(List.of(record1.getRight()), got.get(1).getRecordsList());
    Assertions.assertEquals(ContinuationTokenInternal.newBuilder()
            .putShardHorizonSequenceNumbers("shard1", "0001")
                .build(), ContinuationTokenInternal.parseFrom(got.get(1).getContinuationToken()));
    assertEquals(AccountDataStream.newBuilder().setDatasetCurrent(true).build(), got.get(2));

    // Stream should not complete
    assertThrows(TimeoutException.class, () -> sc.completed.get(1, TimeUnit.SECONDS));
  }


  @Test
  void testRateLimit() throws Throwable {
    // AccountStream should retry when it encounters any kinesis rate limit
    when(accountDB.listAccounts()).thenReturn(List.of());

    var p = record(
        "0000",
        11,
        UUID.fromString("11111111-1111-1111-1111-111111111111"),
        UUID.fromString("11111111-1111-1111-1111-222222222222"),
        new byte[16]);
    final software.amazon.awssdk.services.kinesis.model.Record kinesisRecord = p.getLeft();
    final Record expected = p.getRight();


    // rate limit the first describeStream request
    when(kinesis.describeStream((DescribeStreamRequest) any()))
        .thenReturn(CompletableFuture.failedFuture(ProvisionedThroughputExceededException.builder().build()))
        .thenReturn(CompletableFuture.completedFuture(
            DescribeStreamResponse.builder()
                .streamDescription(StreamDescription.builder()
                    .shards(software.amazon.awssdk.services.kinesis.model.Shard.builder()
                        .shardId("shard1")
                        .sequenceNumberRange(SequenceNumberRange.builder()
                            .startingSequenceNumber("0000")
                            .build())
                        .build())
                    .build())
                .build()));

    // rate limit the first getShardIterator request
    when(kinesis.getShardIterator(GetShardIteratorRequest.builder()
        .streamName(streamName)
        .shardId("shard1")
        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
        .build()))
        .thenReturn(CompletableFuture.failedFuture(ProvisionedThroughputExceededException.builder().build()))
        .thenReturn(
            CompletableFuture.completedFuture(GetShardIteratorResponse.builder().shardIterator("sharditer1").build()));

    // rate limit the first getRecords request
    when(kinesis.getRecords(GetRecordsRequest.builder()
        .shardIterator("sharditer1")
        .build()))
        .thenReturn(CompletableFuture.failedFuture(ProvisionedThroughputExceededException.builder().build()))
        .thenReturn(CompletableFuture.completedFuture(GetRecordsResponse.builder()
            .records(kinesisRecord).build()));

    when(kinesis.getShardIterator(GetShardIteratorRequest.builder()
        .streamName(streamName)
        .shardId("shard1")
        .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
        .startingSequenceNumber("0000")
        .build()))
        .thenReturn(CompletableFuture.completedFuture(GetShardIteratorResponse.builder()
                .shardIterator("sharditer2")
                .build()),
            new CompletableFuture<GetShardIteratorResponse>());
    when(kinesis.getRecords(GetRecordsRequest.builder()
        .shardIterator("sharditer2")
        .build())).thenReturn(CompletableFuture.completedFuture(GetRecordsResponse.builder()
        .records(Collections.EMPTY_LIST)
        .build()));
    StreamCollector sc = new StreamCollector();

    stub.streamAccountData(AccountDataRequest.newBuilder()
        .setShard(Shard.newBuilder()
            .setLimit(ByteString.copyFrom(UUIDUtil.byteBufferFromUUID(UUID.fromString("20000000-0000-0000-0000-000000000000"))))
            .build())
        .build(), sc);
    final List<AccountDataStream> got = sc.results.get();
    assertEquals(3, got.size());
    assertEquals(AccountDataStream.newBuilder().setClearAll(true).build(), got.get(0));
    assertEquals(List.of(expected), got.get(1).getRecordsList());
    Assertions.assertEquals(ContinuationTokenInternal.newBuilder()
        .putShardHorizonSequenceNumbers("shard1", "0000")
        .build(), ContinuationTokenInternal.parseFrom(got.get(1).getContinuationToken()));
    assertEquals(AccountDataStream.newBuilder().setDatasetCurrent(true).build(), got.get(2));

    // Stream should not complete
    assertThrows(TimeoutException.class, () -> sc.completed.get(1, TimeUnit.SECONDS));
  }
}
