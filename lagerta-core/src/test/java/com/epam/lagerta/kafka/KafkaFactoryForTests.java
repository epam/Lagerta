package com.epam.lagerta.kafka;

import com.epam.lagerta.BaseIntegrationTest;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

public class KafkaFactoryForTests implements KafkaFactory {
    private final KafkaFactory kafkaFactory;

    public KafkaFactoryForTests(KafkaFactory kafkaFactory) {
        this.kafkaFactory = kafkaFactory;
    }

    @SuppressWarnings("unchecked")
    public <K, V> Producer<K, V> producer(Properties properties) {
        return new ProducerProxy<>(kafkaFactory.producer(properties));
    }

    public <K, V> Consumer<K, V> consumer(Properties properties) {
        return kafkaFactory.consumer(properties);
    }


    private static class ProducerProxy<K, V> implements Producer<K, V> {
        private final Producer<K, V> producer;

        ProducerProxy(Producer<K, V> producer) {
            this.producer = producer;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
            return send(record, null);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
            ProducerRecord<K, V> adjustedRecord = new ProducerRecord<K, V>(
                BaseIntegrationTest.adjustTopicNameForTest(record.topic()),
                record.partition(),
                record.timestamp(),
                record.key(),
                record.value()
            );
            return producer.send(adjustedRecord, callback);
        }

        @Override
        public void flush() {
            producer.flush();
        }


        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return producer.partitionsFor(BaseIntegrationTest.adjustTopicNameForTest(topic));
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return producer.metrics();
        }

        @Override
        public void close() {
            producer.close();
        }

        @Override
        public void close(long timeout, TimeUnit unit) {
            producer.close(timeout, unit);
        }
    }
}
