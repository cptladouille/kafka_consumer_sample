package com.cnam.kafka_demo.kafka_receiver_api.kafka;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyConsumer extends ShutdownableThread{

    private final KafkaConsumer<String, Integer> consumer;

    private final String _topicName;

    public static final String KAFKA_SERVER_URL = "localhost";

    public static final int KAFKA_SERVER_PORT = 9092;

    public static final String CLIENT_ID = "SampleConsumer";

    public MyConsumer(String topicName) {
        super("MyConsumer",false);

        Properties properties = new Properties();

        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);

        properties.put("client.id", CLIENT_ID);
        properties.put("group.id", CLIENT_ID);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, Integer>(properties);

        this._topicName = topicName;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this._topicName));
        ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofSeconds(10));
        for (ConsumerRecord record : records) {
            System.out.println("Message recu: (" + record.key() + ", " + record.value() + ") a la position " + record.offset());
        }
    }
    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }

}
