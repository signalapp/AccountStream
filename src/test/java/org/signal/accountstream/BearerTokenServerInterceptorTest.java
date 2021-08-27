/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.grpc.annotation.GrpcChannel;
import io.micronaut.grpc.server.GrpcServerChannel;
import io.micronaut.runtime.context.scope.Refreshable;
import io.micronaut.runtime.http.scope.RequestScope;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.junit.jupiter.api.Test;
import org.signal.accountstream.accountdb.AccountDB;
import org.signal.accountstream.proto.AccountDataRequest;
import org.signal.accountstream.proto.AccountDataStream;
import org.signal.accountstream.proto.AccountStreamServiceGrpc;
import org.signal.accountstream.proto.AccountStreamServiceGrpc.AccountStreamServiceStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@MicronautTest
@Property(name = "grpc.server.secret-token", value = "abc")
class BearerTokenServerInterceptorTest {
  private static final Logger logger = LoggerFactory.getLogger(BearerTokenServerInterceptorTest.class);

  @MockBean(AccountDB.class)
  AccountDB accountDB() {
    return mock(AccountDB.class);
  }

  @Inject
  AccountDB db;

  @MockBean(KinesisAsyncClient.class)
  KinesisAsyncClient kinesisAsyncClient() {
    return mock(KinesisAsyncClient.class);
  }

  @Inject
  @GrpcChannel(GrpcServerChannel.NAME)
  ManagedChannel channel;

  private static class BearerTokenCredentials extends CallCredentials {
    private final String secretToken;

    BearerTokenCredentials(String secretToken) {
      super();
      this.secretToken = secretToken;
    }

    @Override
    public void applyRequestMetadata(final RequestInfo requestInfo, final Executor executor,
        final MetadataApplier metadataApplier) {
      Metadata md = new Metadata();
      md.put(BearerTokenServerInterceptor.AUTHORIZATION, secretToken);
      metadataApplier.apply(md);
    }

    @Override
    public void thisUsesUnstableApi() {}
  }

  @Test
  void noToken() throws InterruptedException {
    CountDownLatch done = new CountDownLatch(1);
    var stub = AccountStreamServiceGrpc.newStub(channel);
    stub.streamAccountData(AccountDataRequest.newBuilder().build(), new StreamObserver<AccountDataStream>() {
      @Override
      public void onNext(final AccountDataStream accountDataStream) {
        fail("should not have received messages");
      }

      @Override
      public void onError(final Throwable throwable) {
        assertEquals(Code.UNAUTHENTICATED, Status.fromThrowable(throwable).getCode());
        done.countDown();
      }

      @Override
      public void onCompleted() {
        assertTrue(false, "should not have succeeded");
      }
    });
    done.await();
  }

  @Test
  void testIncorrectToken() throws InterruptedException {
    CountDownLatch done = new CountDownLatch(1);
    var stub = AccountStreamServiceGrpc.newStub(channel);
    stub.withCallCredentials(new BearerTokenCredentials("def"))
        .streamAccountData(AccountDataRequest.newBuilder().build(), new StreamObserver<AccountDataStream>() {
          @Override
          public void onNext(final AccountDataStream accountDataStream) {
            assertTrue(false, "should not have received messages");
          }

          @Override
          public void onError(final Throwable throwable) {
            assertEquals(Code.UNAUTHENTICATED, Status.fromThrowable(throwable).getCode());
            done.countDown();
          }

          @Override
          public void onCompleted() {
            assertTrue(false, "should not have succeeded");
          }
        });
    done.await();
  }

  @Test
  void testCorrectToken() throws InterruptedException {
    when(db.listAccounts()).thenThrow(Status.fromCode(Code.CANCELLED).asRuntimeException());
    CountDownLatch done = new CountDownLatch(1);
    var stub = AccountStreamServiceGrpc.newStub(channel);
    stub.withCallCredentials(new BearerTokenCredentials("abc"))
        .streamAccountData(AccountDataRequest.newBuilder().build(), new StreamObserver<AccountDataStream>() {
      @Override
      public void onNext(final AccountDataStream accountDataStream) {
        assertTrue(false, "should not have received messages");
      }

      @Override
      public void onError(final Throwable throwable) {
        assertEquals(Code.CANCELLED, Status.fromThrowable(throwable).getCode());
        done.countDown();
      }

      @Override
      public void onCompleted() {
        assertTrue(false, "should not have succeeded");
      }
    });
    done.await();
  }
}
