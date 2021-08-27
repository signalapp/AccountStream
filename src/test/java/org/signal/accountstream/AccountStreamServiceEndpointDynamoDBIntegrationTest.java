/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.signal.accountstream.AccountStreamServiceEndpoint.nextPageToStream;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Value;
import io.micronaut.grpc.annotation.GrpcChannel;
import io.micronaut.grpc.server.GrpcServerChannel;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.signal.accountstream.AccountStream.AccountUpdateStreamPage;
import org.signal.accountstream.accountdb.AccountDB;
import org.signal.accountstream.proto.AccountDataRequest;
import org.signal.accountstream.proto.AccountDataStream;
import org.signal.accountstream.proto.AccountDataStream.Record;
import org.signal.accountstream.proto.AccountStreamServiceGrpc;
import org.signal.accountstream.proto.AccountStreamServiceGrpc.AccountStreamServiceStub;
import org.signal.accountstream.proto.ContinuationTokenInternal;
import org.signal.accountstream.proto.Shard;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import javax.annotation.Nullable;

@MicronautTest
class AccountStreamServiceEndpointDynamoDBIntegrationTest {

  @Factory
  static class AccountStreamServiceTestClientFactory {
    @Bean
    @Replaces(AccountStreamServiceStub.class)
    AccountStreamServiceStub stub(
        @GrpcChannel(GrpcServerChannel.NAME) final ManagedChannel channel) {

      return AccountStreamServiceGrpc.newStub(channel);
    }
  }

  @Inject
  AccountStreamServiceGrpc.AccountStreamServiceStub stub;

  private static final String ACCOUNTS_TABLE_NAME = "accounts";

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName(ACCOUNTS_TABLE_NAME)
      .hashKey(AccountDB.KEY_ACCOUNT_UUID)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(AccountDB.KEY_ACCOUNT_UUID)
          .attributeType(ScalarAttributeType.B)
          .build())
      .build();

  private static class AccountData {
    @JsonProperty
    UUID pni;
    @JsonProperty
    byte[] uak;
  }

  void writeAccount(long e164, UUID aci, UUID pni, @Nullable byte[] uak, Optional<Boolean> discoverable) throws Exception {
    Map<String, AttributeValue> fields = new HashMap<>();
    fields.put(AccountDB.KEY_ACCOUNT_UUID, AttributeValue.builder().b(SdkBytes.fromByteBuffer(UUIDUtil.byteBufferFromUUID(aci))).build());
    fields.put(AccountDB.ATTR_ACCOUNT_E164, AttributeValue.builder().s("+" + Long.toString(e164, 10)).build());
    if (discoverable.isPresent()) {
      fields.put(AccountDB.ATTR_CANONICALLY_DISCOVERABLE, AttributeValue.builder().bool(discoverable.get()).build());
    }
    if (pni != null) {
      fields.put(AccountDB.ATTR_PNI, AttributeValue.builder().b(SdkBytes.fromByteBuffer(UUIDUtil.byteBufferFromUUID(pni))).build());
    }
    if (uak != null) {
      fields.put(AccountDB.ATTR_UAK, AttributeValue.builder().b(SdkBytes.fromByteArray(uak)).build());
    }
    dynamoDbExtension.getDynamoDbClient().putItem(PutItemRequest.builder()
        .tableName(ACCOUNTS_TABLE_NAME)
        .item(fields)
        .build());
  }

  @MockBean(DynamoDbClient.class)
  DynamoDbClient dynamoDb() {
    return dynamoDbExtension.getDynamoDbClient();
  }

  @MockBean(AccountStream.class)
  AccountStream accountStream() {
    return mock(AccountStream.class);
  }

  @Inject
  AccountStream accountStream;

  @Value("${accountdb.streamname}")
  String streamName;

  @Test
  void testBasic() throws Throwable {
    when(accountStream.streamNextUpdates(any())).thenReturn(
        new AccountUpdateStreamPage(List.of(), ContinuationTokenInternal.getDefaultInstance(), false));
    when(accountStream.beginningOfStreamContinuationToken()).thenReturn(ContinuationTokenInternal.getDefaultInstance());

    UUID aci11 = UUID.fromString("11111111-1111-1111-1111-111111111111");
    UUID aci12 = UUID.fromString("11111111-1111-1111-1111-111111111112");
    UUID aci13 = UUID.fromString("11111111-1111-1111-1111-111111111113");
    UUID aci14 = UUID.fromString("11111111-1111-1111-1111-111111111114");
    UUID pni11 = UUID.fromString("11111111-2222-1111-1111-111111111111");
    UUID pni12 = UUID.fromString("11111111-2222-1111-1111-111111111112");
    UUID pni13 = UUID.fromString("11111111-2222-1111-1111-111111111113");
    UUID pni14 = UUID.fromString("11111111-2222-1111-1111-111111111114");
    byte[] uak11 = new byte[]{1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6};
    writeAccount(11, aci11, pni11, uak11, Optional.of(true));
    writeAccount(12, aci12, pni12, null, Optional.of(false));
    writeAccount(13, aci13, pni13, null, Optional.of(true));
    writeAccount(14, aci14, pni14, null, Optional.empty());  // "C" column not in DB at all, should assume false

    // This won't show up in output, since it's out of the requested UUID range.
    UUID aci21 = UUID.fromString("21111111-1111-1111-1111-111111111111");
    UUID pni21 = UUID.fromString("21111111-2222-1111-1111-111111111111");
    writeAccount(21, aci21, pni21, null, Optional.of(true));

    final int parallelRequests = 10;
    CountDownLatch completed[] = new CountDownLatch[parallelRequests];
    CountDownLatch streamFinished[] = new CountDownLatch[parallelRequests];
    final Throwable[] error = new Throwable[parallelRequests];
    final List<AccountDataStream>[] got = new List[parallelRequests];
    final ClientCallStreamObserver<AccountDataRequest>[] clientFeatures = new ClientCallStreamObserver[10];

    for (int i = 0; i < parallelRequests; i++) {
      final int index = i;
      completed[i] = new CountDownLatch(1);
      streamFinished[i] = new CountDownLatch(1);
      got[i] = new ArrayList<>();
      StreamObserver<AccountDataStream> observer = new ClientResponseObserver<AccountDataRequest, AccountDataStream>() {
        @Override
        public void onNext(final AccountDataStream accountDataStream) {
          got[index].add(accountDataStream);
          if (accountDataStream.getDatasetCurrent())
            completed[index].countDown();
        }

        @Override
        public void onError(final Throwable throwable) {
          error[index] = throwable;
          completed[index].countDown();
          streamFinished[index].countDown();
        }

        @Override
        public void onCompleted() {
          onError(new RuntimeException("did not expect stream to complete"));
        }

        @Override
        public void beforeStart(final ClientCallStreamObserver<AccountDataRequest> clientCallStreamObserver) {
          clientFeatures[index] = clientCallStreamObserver;
        }
      };

      stub.streamAccountData(AccountDataRequest.newBuilder()
          .setShard(Shard.newBuilder()
              .setLimit(ByteString.copyFrom(
                  UUIDUtil.byteBufferFromUUID(UUID.fromString("20000000-0000-0000-0000-000000000000"))))
              .build())
          .build(), observer);
    }
    for (int i = 0; i < parallelRequests; i++) {
      completed[i].await();
      if (error[i] != null)
        throw error[i];

      assertEquals(3, got[i].size());
      assertEquals(AccountDataStream.newBuilder().setClearAll(true).build(), got[i].get(0));
      // Initial snapshot
      assertEquals(Set.of(
              Record.newBuilder()
                  .setE164(11)
                  .setAci(ByteString.copyFrom(UUIDUtil.byteBufferFromUUID(aci11)))
                  .setPni(ByteString.copyFrom(UUIDUtil.byteBufferFromUUID(pni11)))
                  .setUak(ByteString.copyFrom(uak11))
                  .build(),
              Record.newBuilder()
                  .setE164(13)
                  .setAci(ByteString.copyFrom(UUIDUtil.byteBufferFromUUID(aci13)))
                  .setPni(ByteString.copyFrom(UUIDUtil.byteBufferFromUUID(pni13)))
                  .build()),
          got[i].get(1).getRecordsList().stream().collect(Collectors.toSet()));
      assertEquals(AccountDataStream.getDefaultInstance(), got[i].get(1).toBuilder().clearRecords().build());
      // Reach the end, get success.
      assertEquals(AccountDataStream.newBuilder().setDatasetCurrent(true).build(), got[i].get(2));

      // Stream should not complete
      streamFinished[i].await(100, TimeUnit.MILLISECONDS);
      assertTrue(streamFinished[i].getCount() > 0);

      clientFeatures[i].cancel("done", null);
      streamFinished[i].await();
    }
  }

  @Test
  void testNextPageToStream() throws Exception {
    StreamObserver<AccountDataStream> stream = mock(StreamObserver.class);
    AccountUpdateStreamPage page = new AccountUpdateStreamPage(
        List.of(
            new Account(1, null, false, null, null),
            new Account(2, null, false, null, null),
            new Account(3, null, false, null, null),
            new Account(4, null, false, null, null),
            new Account(5, null, false, null, null)),
        ContinuationTokenInternal.newBuilder().build(),
        true);
    nextPageToStream(page, stream, true, 2);

    ArgumentCaptor<AccountDataStream> captor = ArgumentCaptor.forClass(AccountDataStream.class);
    verify(stream, times(3)).onNext(captor.capture());
    List<AccountDataStream> captured = captor.getAllValues();
    assertEquals(captured, List.of(
        AccountDataStream.newBuilder()
            .addRecords(Record.newBuilder().setE164(1).build())
            .addRecords(Record.newBuilder().setE164(2).build())
            .build(),
        AccountDataStream.newBuilder()
            .addRecords(Record.newBuilder().setE164(3).build())
            .addRecords(Record.newBuilder().setE164(4).build())
            .build(),
        AccountDataStream.newBuilder()
            .addRecords(Record.newBuilder().setE164(5).build())
            .setDatasetCurrent(true)
            .build()));
  }
}
