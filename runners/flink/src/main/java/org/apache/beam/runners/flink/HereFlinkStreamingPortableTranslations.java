/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.NativeTransforms;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.flink.FlinkStreamingPortablePipelineTranslator.PTransformTranslator;
import org.apache.beam.runners.flink.FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HereFlinkStreamingPortableTranslations {

  private static final Logger logger =
      LoggerFactory.getLogger(HereFlinkStreamingPortableTranslations.class.getName());

  private static final String FLINK_KAFKA_URN = "here:flinkKafkaInput";
  private static final String FLINK_KAFKA_SINK_URN = "here:flinkKafkaSink";

  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class IsFlinkNativeTransform implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return FLINK_KAFKA_URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform))
          || FLINK_KAFKA_SINK_URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }

  public void addTo(
      ImmutableMap.Builder<String, PTransformTranslator<StreamingTranslationContext>>
          translatorMap) {
    translatorMap.put(FLINK_KAFKA_URN, this::translateKafkaInput);
    translatorMap.put(FLINK_KAFKA_SINK_URN, this::translateKafkaSink);
  }

  private void translateKafkaInput(
      String id,
      RunnerApi.Pipeline pipeline,
      FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);

    final Map<String, Object> params;
    try {
      ObjectMapper mapper = new ObjectMapper();
      params = mapper.readValue(pTransform.getSpec().getPayload().toByteArray(), Map.class);
    } catch (IOException e) {
      throw new RuntimeException("Could not parse KafkaConsumer properties.", e);
    }

    final String topic;
    Preconditions.checkNotNull(topic = (String) params.get("topic"), "'topic' needs to be set");

    Map<?, ?> consumerProps = (Map) params.get("properties");
    Preconditions.checkNotNull(consumerProps, "'properties' need to be set");
    final Properties properties = new Properties();
    properties.putAll(consumerProps);

    logger.info("Kafka consumer for topic {} with properties {}", topic, properties);

    FlinkKafkaConsumer011<WindowedValue<byte[]>> kafkaSource =
        new FlinkKafkaConsumer011<>(topic, new ByteArrayWindowedValueSchema(), properties);

    if (params.getOrDefault("start_from_timestamp_millis", null) != null) {
      kafkaSource.setStartFromTimestamp(
          Long.parseLong(params.get("start_from_timestamp_millis").toString()));
    } else {
      kafkaSource.setStartFromLatest();
    }

    if (params.containsKey("max_out_of_orderness_millis")) {
      Number maxOutOfOrdernessMillis = (Number) params.get("max_out_of_orderness_millis");
      if (maxOutOfOrdernessMillis != null) {
        kafkaSource.assignTimestampsAndWatermarks(
            new WindowedTimestampExtractor<>(
                Time.milliseconds(maxOutOfOrdernessMillis.longValue())));
      }
    }

    context.addDataStream(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        context
            .getExecutionEnvironment()
            .addSource(kafkaSource, FlinkKafkaConsumer011.class.getSimpleName() + "-" + topic));
  }

  /**
   * Deserializer for native Flink Kafka source that produces {@link WindowedValue} expected by Beam
   * operators.
   */
  private static class ByteArrayWindowedValueSchema
      implements KeyedDeserializationSchema<WindowedValue<byte[]>> {
    private static final long serialVersionUID = -1L;

    private final TypeInformation<WindowedValue<byte[]>> ti;

    public ByteArrayWindowedValueSchema() {
      this.ti =
          new CoderTypeInformation<>(
              WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));
    }

    @Override
    public TypeInformation<WindowedValue<byte[]>> getProducedType() {
      return ti;
    }

    @Override
    public WindowedValue<byte[]> deserialize(
        byte[] messageKey, byte[] message, String topic, int partition, long offset) {
      throw new UnsupportedOperationException();
    }

    @Override
    public WindowedValue<byte[]> deserialize(ConsumerRecord<byte[], byte[]> record) {
      return WindowedValue.timestampedValueInGlobalWindow(
          record.value(), new Instant(record.timestamp()));
    }

    @Override
    public boolean isEndOfStream(WindowedValue<byte[]> nextElement) {
      return false;
    }
  }

  // TODO: translation assumes byte[] values, does not support keys and headers
  private void translateKafkaSink(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);
    final String topic;
    final Properties properties = new Properties();
    ObjectMapper mapper = new ObjectMapper();
    try {
      Map<String, Object> params =
          mapper.readValue(pTransform.getSpec().getPayload().toByteArray(), Map.class);

      Preconditions.checkNotNull(topic = (String) params.get("topic"), "'topic' needs to be set");

      Map<?, ?> consumerProps = (Map) params.get("properties");
      Preconditions.checkNotNull(consumerProps, "'properties' need to be set");
      properties.putAll(consumerProps);
    } catch (IOException e) {
      throw new RuntimeException("Could not parse KafkaConsumer properties.", e);
    }

    logger.info("Kafka producer for topic {} with properties {}", topic, properties);

    String inputCollectionId = Iterables.getOnlyElement(pTransform.getInputsMap().values());
    DataStream<WindowedValue<byte[]>> inputDataStream =
        context.getDataStreamOrThrow(inputCollectionId);

    FlinkKafkaProducer011<WindowedValue<byte[]>> producer =
        new FlinkKafkaProducer011<>(topic, new ByteArrayWindowedValueSerializer(), properties);
    // assigner below sets the required Flink record timestamp
    producer.setWriteTimestampToKafka(true);
    inputDataStream
        .transform("setTimestamp", inputDataStream.getType(), new FlinkTimestampAssigner<>())
        .addSink(producer)
        .name(FlinkKafkaProducer011.class.getSimpleName() + "-" + topic);
  }

  public static class ByteArrayWindowedValueSerializer
      implements SerializationSchema<WindowedValue<byte[]>> {
    @Override
    public byte[] serialize(WindowedValue<byte[]> element) {
      return element.getValue();
    }
  }

  /**
   * Assign the timestamp of {@link WindowedValue} as the Flink record timestamp. The Flink
   * timestamp is otherwise not set by the Beam Flink operators but is necessary for native Flink
   * operators such as the Kafka producer.
   *
   * @param <T>
   */
  private static class FlinkTimestampAssigner<T> extends AbstractStreamOperator<WindowedValue<T>>
      implements OneInputStreamOperator<WindowedValue<T>, WindowedValue<T>> {
    {
      super.setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void processElement(StreamRecord<WindowedValue<T>> element) {
      Instant timestamp = element.getValue().getTimestamp();
      if (timestamp != null && timestamp.getMillis() > 0) {
        super.output.collect(element.replace(element.getValue(), timestamp.getMillis()));
      } else {
        super.output.collect(element);
      }
    }

    @Override
    public void setup(StreamTask containingTask, StreamConfig config, Output output) {
      super.setup(containingTask, config, output);
    }
  }

  private static class WindowedTimestampExtractor<T>
      extends BoundedOutOfOrdernessTimestampExtractor<WindowedValue<T>> {
    public WindowedTimestampExtractor(Time maxOutOfOrderness) {
      super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(WindowedValue<T> element) {
      return element.getTimestamp() != null ? element.getTimestamp().getMillis() : Long.MIN_VALUE;
    }
  }
}
