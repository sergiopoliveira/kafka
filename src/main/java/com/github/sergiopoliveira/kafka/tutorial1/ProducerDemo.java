package com.github.sergiopoliveira.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World");

            // send data - asynchronous
            producer.send(record);
        }

        // not required below, using try-with-resources instead
//        // flush data
//        producer.flush();
//
//        // flush and close data
//        producer.close();

    }
}
