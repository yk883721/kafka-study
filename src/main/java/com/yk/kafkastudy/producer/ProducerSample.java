package com.yk.kafkastudy.producer;

import org.apache.kafka.clients.producer.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerSample {

    private final static String TOPIC_NAME="test-a-topic";

    Producer<String, String> producer;

    @Before
    public void before(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.61.128:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,"0");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.yk.kafkastudy.admin.SamplePartition");

        producer = new KafkaProducer<>(properties);
    }

    @After
    public void after(){
        producer.close();
    }

    @Test
    public void testSend() throws Exception{
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i );
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();

            System.out.println("partition = " + metadata.partition() + ", offset: " + metadata.offset());
        }
    }

    @Test
    public void testSendCallback() throws Exception{
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i );
            producer.send(record,
                (metadata, exception)
                        -> System.out.println(
                                "partition = " + metadata.partition() + ", offset: " + metadata.offset()));

        }
    }

}
