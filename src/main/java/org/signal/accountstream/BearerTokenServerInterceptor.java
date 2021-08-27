/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.order.Ordered;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

@Singleton
public class BearerTokenServerInterceptor implements ServerInterceptor, Ordered {
  private static final Logger logger = LoggerFactory.getLogger(BearerTokenServerInterceptor.class);

  private final String secretToken;
  public static final Metadata.Key<String> AUTHORIZATION = Metadata.Key.of("x-signal-auth-token", ASCII_STRING_MARSHALLER);

  public BearerTokenServerInterceptor(@Value("${grpc.server.secret-token:}") String secretToken) {
    this.secretToken = secretToken;
    if (secretToken.isBlank()) {
      logger.error("NO AUTHENTICATION SET UP ON THIS SERVER");
    } else {
      logger.info("Using bearer-token authentication");
    }
  }

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall, final Metadata metadata,
      final ServerCallHandler<ReqT, RespT> next) {
    if (secretToken.equals("")) {
      return next.startCall(serverCall, metadata);  // authentication off, just do it.
    }
    String value = metadata.get(AUTHORIZATION);
    final Status status;
    if (value == null) {
      status = Status.UNAUTHENTICATED.withDescription("no authorization");
    } else if (!secretToken.equals(value)) {
      status = Status.UNAUTHENTICATED.withDescription("bad token");
    } else {
      // Auth succeeded, actually handle the call.
      return next.startCall(serverCall, metadata);
    }
    logger.warn("Bearer-token authentication failed for client");
    serverCall.close(status, metadata);
    return new ServerCall.Listener<>() { /* noop */ };
  }

  @Override
  public int getOrder() {
    return 1;
  }
}
