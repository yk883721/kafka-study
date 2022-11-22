package com.yk.kafkastudy.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

public class ConsumerSample {

    private final String TOPIC_NAME = "test-topic";
    private KafkaConsumer<String, String> consumer;

    @Before
    public void setUp(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.6:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);

//        consumer.subscribe(Collections.singleton(TOPIC_NAME));

        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);
        consumer.assign(Collections.singleton(p0));

        consumer.seek(p0, 10);

    }

    @Test
    public void testConsumer(){
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf(
                        "partition = %d, offset = %d, key = %s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
        }
    }

    @Test
    public void testCommit(){
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records){
                // 数据处理，若失败，则不要提交 offset
                System.out.printf(
                        "partition = %d, offset = %d, key = %s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
            consumer.commitAsync();
        }
    }

    @Test
    public void testCommitPartition(){

        long totalNum = 40;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> pRecords = records.records(partition);

                long num = 0;
                for (ConsumerRecord<String, String> record : pRecords){
                    // 数据处理，若失败，则不要提交 offset
                    System.out.printf(
                            "partition = %d, offset = %d, key = %s, value = %s%n",
                            record.partition(), record.offset(), record.key(), record.value());

                    num++;
                    if (record.partition() == 0) {
                        if (num >= totalNum) {
                            consumer.pause(Collections.singleton(new TopicPartition(TOPIC_NAME, 0)));
                        }
                    }
                    if (record.partition() == 1) {
                        if (num == 40) {
                            consumer.resume(Collections.singleton(new TopicPartition(TOPIC_NAME, 0)));
                        }
                    }
                }

                Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
                long offset = pRecords.get(pRecords.size() - 1).offset();
                // 提交的 offset 为下次消费的起点，需要 + 1
                offsetMap.put(partition, new OffsetAndMetadata(offset + 1));
                consumer.commitSync(offsetMap);
            }
        }
    }

}
