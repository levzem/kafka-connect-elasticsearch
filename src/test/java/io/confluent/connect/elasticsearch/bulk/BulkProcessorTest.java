/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/
package io.confluent.connect.elasticsearch.bulk;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static io.confluent.connect.elasticsearch.bulk.BulkProcessor.BehaviorOnMalformedDoc;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class BulkProcessorTest {

  private static class Expectation {
    final List<Integer> request;
    final BulkResponse response;

    private Expectation(List<Integer> request, BulkResponse response) {
      this.request = request;
      this.response = response;
    }
  }

  private static final class Client implements BulkClient<Integer, List<Integer>> {
    private final Queue<Expectation> expectQ = new LinkedList<>();
    private volatile boolean executeMetExpectations = true;

    @Override
    public List<Integer> bulkRequest(List<Integer> batch) {
      List<Integer> ids = new ArrayList<>(batch.size());
      for (Integer id : batch) {
        ids.add(id);
      }
      return ids;
    }

    public void expect(List<Integer> ids, BulkResponse response) {
      expectQ.add(new Expectation(ids, response));
    }

    public boolean expectationsMet() {
      return expectQ.isEmpty() && executeMetExpectations;
    }

    @Override
    public BulkResponse execute(List<Integer> request) throws IOException {
      final Expectation expectation;
      try {
        expectation = expectQ.remove();
        assertEquals(expectation.request, request);
      } catch (Throwable t) {
        executeMetExpectations = false;
        throw t;
      }
      executeMetExpectations &= true;
      return expectation.response;
    }
  }

  Client client;
  private Map<String, String> props;

  @Before
  public void createClient() {
    client = new Client();
    props = new HashMap<>();
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "100");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "5");
    props.put(BATCH_SIZE_CONFIG, "2");
    props.put(LINGER_MS_CONFIG, "5");
    props.put(MAX_RETRIES_CONFIG, "3");
    props.put(RETRY_BACKOFF_MS_CONFIG, "1");
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.DEFAULT.name());
    props.put(TYPE_NAME_CONFIG, "dummy");
    props.put(CONNECTION_URL_CONFIG, "dummy");
  }

  @After
  public void checkClient() {
    assertTrue(client.expectationsMet());
  }

  @Test
  public void batchingAndLingering() throws InterruptedException, ExecutionException {
    props.put(BATCH_SIZE_CONFIG, "5");
    props.put(MAX_RETRIES_CONFIG, "0");
    props.put(RETRY_BACKOFF_MS_CONFIG, "0");

    final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        new ElasticsearchSinkConnectorConfig(props)
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(1, addTimeoutMs);
    bulkProcessor.add(2, addTimeoutMs);
    bulkProcessor.add(3, addTimeoutMs);
    bulkProcessor.add(4, addTimeoutMs);
    bulkProcessor.add(5, addTimeoutMs);
    bulkProcessor.add(6, addTimeoutMs);
    bulkProcessor.add(7, addTimeoutMs);
    bulkProcessor.add(8, addTimeoutMs);
    bulkProcessor.add(9, addTimeoutMs);
    bulkProcessor.add(10, addTimeoutMs);
    bulkProcessor.add(11, addTimeoutMs);
    bulkProcessor.add(12, addTimeoutMs);

    client.expect(Arrays.asList(1, 2, 3, 4, 5), BulkResponse.success());
    client.expect(Arrays.asList(6, 7, 8, 9, 10), BulkResponse.success());
    client.expect(Arrays.asList(11, 12), BulkResponse.success()); // batch not full, but upon linger timeout
    assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
    assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
    assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
  }

  @Test
  public void flushing() {
    props.put(BATCH_SIZE_CONFIG, "5");
    props.put(LINGER_MS_CONFIG, "100000"); // super high on purpose to make sure flush is what's causing the request
    props.put(MAX_RETRIES_CONFIG, "0");
    props.put(RETRY_BACKOFF_MS_CONFIG, "0");

    final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        new ElasticsearchSinkConnectorConfig(props)
    );

    client.expect(Arrays.asList(1, 2, 3), BulkResponse.success());

    bulkProcessor.start();

    final int addTimeoutMs = 10;
    bulkProcessor.add(1, addTimeoutMs);
    bulkProcessor.add(2, addTimeoutMs);
    bulkProcessor.add(3, addTimeoutMs);

    assertFalse(client.expectationsMet());

    final int flushTimeoutMs = 100;
    bulkProcessor.flush(flushTimeoutMs);
  }

  @Test
  public void addBlocksWhenBufferFull() {
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "1");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(BATCH_SIZE_CONFIG, "1");
    props.put(LINGER_MS_CONFIG, "10");
    props.put(MAX_RETRIES_CONFIG, "0");
    props.put(RETRY_BACKOFF_MS_CONFIG, "0");

    final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        new ElasticsearchSinkConnectorConfig(props)
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(42, addTimeoutMs);
    assertEquals(1, bulkProcessor.bufferedRecords());
    try {
      // BulkProcessor not started, so this add should timeout & throw
      bulkProcessor.add(43, addTimeoutMs);
      fail();
    } catch (ConnectException good) {
    }
  }

  @Test
  public void retriableErrors() throws InterruptedException, ExecutionException {
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retiable error"));
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retriable error again"));
    client.expect(Arrays.asList(42, 43), BulkResponse.success());

    final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        new ElasticsearchSinkConnectorConfig(props)
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(42, addTimeoutMs);
    bulkProcessor.add(43, addTimeoutMs);

    assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
  }

  @Test
  public void retriableErrorsHitMaxRetries() throws InterruptedException {
    props.put(MAX_RETRIES_CONFIG, "2");
    final String errorInfo = "a final retriable error again";

    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retiable error"));
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retriable error again"));
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, errorInfo));

    final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        new ElasticsearchSinkConnectorConfig(props)
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(42, addTimeoutMs);
    bulkProcessor.add(43, addTimeoutMs);

    try {
      bulkProcessor.submitBatchWhenReady().get();
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause().getMessage().contains(errorInfo));
    }
  }

  @Test
  public void unretriableErrors() throws InterruptedException {
    final String errorInfo = "an unretriable error";
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo));

    final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        new ElasticsearchSinkConnectorConfig(props)
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(42, addTimeoutMs);
    bulkProcessor.add(43, addTimeoutMs);

    try {
      bulkProcessor.submitBatchWhenReady().get();
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause().getMessage().contains(errorInfo));
    }
  }

  @Test
  public void failOnMalformedDoc() {
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.FAIL.name());

    final String errorInfo = " [{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse\"," +
        "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"object\n" +
        " field starting or ending with a [.] makes object resolution ambiguous: [avjpz{{.}}wjzse{{..}}gal9d]\"}}]";
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo));

    final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        new ElasticsearchSinkConnectorConfig(props)
    );

    bulkProcessor.start();

    bulkProcessor.add(42, 1);
    bulkProcessor.add(43, 1);

    try {
      final int flushTimeoutMs = 1000;
      bulkProcessor.flush(flushTimeoutMs);
      fail();
    } catch(ConnectException e) {
      // expected
      assertTrue(e.getMessage().contains("mapper_parsing_exception"));
    }
  }

  @Test
  public void ignoreOrWarnOnMalformedDoc() {
    // Test both IGNORE and WARN options
    // There is no difference in logic between IGNORE and WARN, except for the logging.
    // Test to ensure they both work the same logically
    final List<BehaviorOnMalformedDoc> behaviorsToTest = new ArrayList<BehaviorOnMalformedDoc>() {{
      add(BehaviorOnMalformedDoc.WARN);
      add(BehaviorOnMalformedDoc.IGNORE);
    }};

    for(BehaviorOnMalformedDoc behaviorOnMalformedDoc : behaviorsToTest)
    {
      final String errorInfo = " [{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse\"," +
          "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"object\n" +
          " field starting or ending with a [.] makes object resolution ambiguous: [avjpz{{.}}wjzse{{..}}gal9d]\"}}]";
      client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo));
      props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, behaviorOnMalformedDoc.name());
      final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
          Time.SYSTEM,
          client,
          new ElasticsearchSinkConnectorConfig(props)
      );

      bulkProcessor.start();

      bulkProcessor.add(42, 1);
      bulkProcessor.add(43, 1);

      try {
        final int flushTimeoutMs = 1000;
        bulkProcessor.flush(flushTimeoutMs);
      } catch (ConnectException e) {
        fail(e.getMessage());
      }
    }
  }

  @Test
  public void farmerTaskPropogatesException() {
    final String errorInfo = "an unretriable error";
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo));

    final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
            Time.SYSTEM,
            client,
            new ElasticsearchSinkConnectorConfig(props)
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(42, addTimeoutMs);
    bulkProcessor.add(43, addTimeoutMs);

    Runnable farmer = bulkProcessor.farmerTask();
    ConnectException e = assertThrows(
            ConnectException.class, () -> farmer.run());
    assertThat(e.getMessage(), containsString(errorInfo));
  }

  @Test
  public void terminateRetriesWhenInterruptedInSleep() {
    Time mockTime = mock(Time.class);
    doAnswer(invocation -> {
      Thread.currentThread().interrupt();
      return null;
    }).when(mockTime).sleep(anyLong());

    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retriable error"));

    final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
            mockTime,
            client,
            new ElasticsearchSinkConnectorConfig(props)
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(42, addTimeoutMs);
    bulkProcessor.add(43, addTimeoutMs);

    ExecutionException e = assertThrows(ExecutionException.class,
            () -> bulkProcessor.submitBatchWhenReady().get());
    assertThat(e.getMessage(), containsString("a retriable error"));
  }
}
