package com.eckyputrady.kafkatest.kafkatestclient;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Properties;

public class KProducer {

    public KProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        try (Producer producer = new KafkaProducer(props)) {
            for (long i = 0; i < 1_000_000L; ++i) {
                for (int j = 1; j <= 3; ++j) {
                    String topic = "test-" + j;
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream daos = new DataOutputStream(baos)) {
                        daos.writeLong(i);
                        daos.writeLong(System.currentTimeMillis());
                        daos.flush();
                        byte[] output = baos.toByteArray();
                        producer.send(new ProducerRecord<>(topic, output, output));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
