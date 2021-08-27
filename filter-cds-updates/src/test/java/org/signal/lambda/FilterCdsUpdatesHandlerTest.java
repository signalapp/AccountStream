/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.signal.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.tests.EventLoader;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

// Modeled after https://aws.amazon.com/blogs/opensource/testing-aws-lambda-functions-written-in-java/
public class FilterCdsUpdatesHandlerTest {

  private static final byte[] uuidBytes = Account.uuidToBytes(UUID.fromString("22222222-2222-2222-2222-222222222222"));

  public void fileInputOutput(String filename, List<Account> expected) throws IOException {
    DynamodbEvent event = EventLoader.loadDynamoDbEvent(filename);
    KinesisClient mockClient = mock(KinesisClient.class);
    FilterCdsUpdatesHandler handler = new FilterCdsUpdatesHandler(mockClient, "mystream");
    Context contextMock = mock(Context.class);
    handler.handleRequest(event, contextMock);
    ArgumentCaptor<PutRecordRequest> captor = ArgumentCaptor.forClass(PutRecordRequest.class);
    verify(mockClient, times(expected.size())).putRecord(captor.capture());
    List<Account> accounts = captor.getAllValues().stream().map(c -> mapWithoutException(c.data()))
        .collect(Collectors.toList());
    assertEquals(expected, accounts);
  }

  @Test
  public void testE164Change() throws IOException {
    fileInputOutput("testevent_numberchange.json",
        List.of(
            new Account("+12223334444", uuidBytes, false, null, null),
            new Account("+13334445555", uuidBytes, true, null, null)));
  }

  @Test
  public void testUAKChange() throws IOException {
    fileInputOutput("testevent_uakchange.json",
        List.of(
            new Account("+12223334444", uuidBytes, true, null,
                new byte[]{0x04, 0x10, 0x41, 0x04, 0x10, 0x41, 0x04, 0x10, 0x41, 0x04, 0x10, 0x41, 0x04, 0x10, 0x41, 0x04})));
  }

  @Test
  public void testPNIChange() throws IOException {
    fileInputOutput("testevent_pnichange.json",
        List.of(
            new Account("+12223334444", uuidBytes, true, uuidBytes, null)));
  }

  Account mapWithoutException(SdkBytes in) {
    try {
      return FilterCdsUpdatesHandler.OBJECT_MAPPER.readValue(in.asInputStream(), Account.class);
    } catch (IOException e) {
      throw new RuntimeException("mapping", e);
    }
  }
}
