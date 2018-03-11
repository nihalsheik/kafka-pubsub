/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nihalsoft.kafka.examples.producer;

import java.util.Properties;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

// https://howtoprogram.xyz/2016/05/29/create-multi-threaded-apache-kafka-consumer/
// https://dzone.com/articles/writing-a-kafka-consumer-in-java

public class BasicPartitionExample {

    public static void main(String[] args) {

        try {

            String topic = "test";
            long noOfMessages = 5;

            System.out.println("Started...");

            Properties producerConfig = new Properties();
            producerConfig.put("bootstrap.servers", "localhost:9092");
            producerConfig.put("client.id", "basic-producer");
            producerConfig.put("acks", "all");
            producerConfig.put("retries", "3");
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArraySerializer");

            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerConfig);
            ProducerRecord<String, byte[]> record;

            for (int i = 0; i < noOfMessages; i++) {
                System.out.println("Sending...");
                record = new ProducerRecord<String, byte[]>(topic, i % 3, "Key=" + i, getValue(i));
                producer.send(record);
            }

            producer.flush();
            producer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static byte[] getValue(int i) {
        return SerializationUtils.serialize(new MyEvent(i, "event" + i, "test", System.currentTimeMillis()));
    }

}