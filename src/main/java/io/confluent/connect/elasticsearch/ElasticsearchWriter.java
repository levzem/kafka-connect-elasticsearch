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

package io.confluent.connect.elasticsearch;

import io.confluent.connect.elasticsearch.bulk.BulkProcessor;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.confluent.connect.elasticsearch.DataConverter.BehaviorOnNullValues;

public class ElasticsearchWriter {
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchWriter.class);

  private final ElasticsearchClient client;
  @Deprecated
  private final Map<String, String> topicToIndexMap;
  private final BulkProcessor<IndexableRecord, ?> bulkProcessor;
  private final DataConverter converter;
  private final Set<String> existingMappings;
  private final ElasticsearchSinkConnectorConfig config;

  ElasticsearchWriter(
      ElasticsearchClient client,
      Map<String, String> topicToIndexMap,
      ElasticsearchSinkConnectorConfig config
  ) {
    this.client = client;
    this.topicToIndexMap = topicToIndexMap;
    this.converter =
        new DataConverter(config.useCompactMapEntries(), config.behaviorOnNullValues());
    this.config = config;

    bulkProcessor = new BulkProcessor<>(new SystemTime(), new BulkIndexingClient(client), config);
    existingMappings = new HashSet<>();
  }

  public void write(Collection<SinkRecord> records) {
    for (SinkRecord sinkRecord : records) {
      // Preemptively skip records with null values if they're going to be ignored anyways
      if (ignoreRecord(sinkRecord)) {
        log.debug(
            "Ignoring sink record with key {} and null value for topic/partition/offset {}/{}/{}",
            sinkRecord.key(),
            sinkRecord.topic(),
            sinkRecord.kafkaPartition(),
            sinkRecord.kafkaOffset());
        continue;
      }

      final String index = convertTopicToIndexName(sinkRecord.topic());
      final boolean ignoreKey =
          config.topicIgnoreKey().contains(sinkRecord.topic()) || config.ignoreKey();
      final boolean ignoreSchema =
          config.topicIgnoreSchema().contains(sinkRecord.topic()) || config.ignoreSchema();

      client.createIndices(Collections.singleton(index));

      if (!ignoreSchema && !existingMappings.contains(index)) {
        try {
          if (Mapping.getMapping(client, index, config.type()) == null) {
            Mapping.createMapping(client, index, config.type(), sinkRecord.valueSchema());
          }
        } catch (IOException e) {
          // FIXME: concurrent tasks could attempt to create the mapping and one of the requests may
          // fail
          throw new ConnectException("Failed to initialize mapping for index: " + index, e);
        }
        existingMappings.add(index);
      }

      tryWriteRecord(sinkRecord, index, ignoreKey, ignoreSchema);
    }
  }

  private boolean ignoreRecord(SinkRecord record) {
    return record.value() == null && config.behaviorOnNullValues() == BehaviorOnNullValues.IGNORE;
  }

  private void tryWriteRecord(
      SinkRecord sinkRecord,
      String index,
      boolean ignoreKey,
      boolean ignoreSchema) {
    IndexableRecord record = null;
    try {
      record = converter.convertRecord(
              sinkRecord,
              index,
              config.type(),
              ignoreKey,
              ignoreSchema);
    } catch (ConnectException convertException) {
      if (config.dropInvalidMessage()) {
        log.error(
            "Can't convert record from topic {} with partition {} and offset {}. "
                + "Error message: {}",
            sinkRecord.topic(),
            sinkRecord.kafkaPartition(),
            sinkRecord.kafkaOffset(),
            convertException.getMessage()
        );
      } else {
        throw convertException;
      }
    }
    if (record != null) {
      bulkProcessor.add(record, config.flushTimeoutMs());
    }
  }

  /**
   * Return the expected index name for a given topic, using the configured mapping or the topic
   * name. Elasticsearch accepts only lowercase index names
   * (<a href="https://github.com/elastic/elasticsearch/issues/29420">ref</a>_.
   */
  private String convertTopicToIndexName(String topic) {
    final String indexOverride = topicToIndexMap.get(topic);
    String index = indexOverride != null ? indexOverride : topic.toLowerCase();
    log.debug("Topic '{}' was translated as index '{}'", topic, index);
    return index;
  }

  public void flush() {
    bulkProcessor.flush(config.flushTimeoutMs());
  }

  public void start() {
    bulkProcessor.start();
  }

  public void stop() {
    try {
      bulkProcessor.flush(config.flushTimeoutMs());
    } catch (Exception e) {
      log.warn("Failed to flush during stop", e);
    }
    bulkProcessor.stop();
    bulkProcessor.awaitStop(config.flushTimeoutMs());
  }

  public void createIndicesForTopics(Set<String> assignedTopics) {
    Objects.requireNonNull(assignedTopics);
    client.createIndices(indicesForTopics(assignedTopics));
  }

  private Set<String> indicesForTopics(Set<String> assignedTopics) {
    final Set<String> indices = new HashSet<>();
    for (String topic : assignedTopics) {
      indices.add(convertTopicToIndexName(topic));
    }
    return indices;
  }
}
