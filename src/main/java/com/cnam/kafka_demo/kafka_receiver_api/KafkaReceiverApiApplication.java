package com.cnam.kafka_demo.kafka_receiver_api;

import com.cnam.kafka_demo.kafka_receiver_api.kafka.MyConsumer;

public class KafkaReceiverApiApplication {

	public static void main(String[] args) {
		MyConsumer consumer = new MyConsumer("test");
		consumer.start();
	}

}
