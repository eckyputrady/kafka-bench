package com.eckyputrady.kafkatest.kafkatestclient;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    public static void main(String[] args) {
        if (Objects.equals(args[1], "consumer")) {
            consumer(args[2], 3);
        } else if (Objects.equals(args[1], "producer")) {
            producer(args[2], 1_000_000L, 3);
        } else {
            System.out.println("unknown params: " + args[1]);
        }
    }


    public static void producer(String servers, long msgCount, int topicCount) {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        try (Producer producer = new KafkaProducer(props)) {
            for (long i = 0; i < msgCount; ++i) {
                for (int j = 1; j <= topicCount; ++j) {
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

    public static void consumer(String servers, int topicCount) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", servers);
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(IntStream.rangeClosed(1, 3).mapToObj(x -> "test-" + x).collect(Collectors.toList()));
            try (DataOutputStream daos = new DataOutputStream(new FileOutputStream("consumer", true))) {
                while (true) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                    long now = System.currentTimeMillis();
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        daos.writeLong(now);
                        daos.write(record.value());
                        daos.writeByte(0);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
