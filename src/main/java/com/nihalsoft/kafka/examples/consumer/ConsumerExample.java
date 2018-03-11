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

package com.nihalsoft.kafka.examples.consumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class ConsumerExample extends Thread {

    public static void main(String[] args) {
        System.out.println("==================== my-group22");
        // new BasicConsumerExample(args[0]).start();
        new ConsumerExample("aa", "mygroupa").start();
        new ConsumerExample("bb", "mygroupa").start();

        new ConsumerExample("cc", "mygroupb").start();
        new ConsumerExample("dd", "mygroupb").start();

    }

    private String name = "";
    private String groupId = "";

    KafkaConsumer<String, byte[]> consumer = null;

    public ConsumerExample(String name, String groupId) {
        this.name = name;
        this.groupId = groupId;
    }

    @Override
    public void run() {

        System.out.println("Started -------->" + this.name);

        try {

            String topic = "test";

            Properties prop = new Properties();
            prop.put("group.id", this.groupId);
            prop.put("bootstrap.servers", "localhost:9092");
            prop.put("auto.offset.reset", "latest");
            // prop.put("auto.commit.interval.ms", 8000);
            prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            prop.put("enable.auto.commit", "false");
            // prop.put("auto.commit.interval.ms", "1000");
            // prop.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

            consumer = new KafkaConsumer<>(prop);
            consumer.subscribe(Collections.singletonList(topic));

            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.printf("%s topic-partitions are revoked from this consumer\n",
                            Arrays.toString(partitions.toArray()));
                }

                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.printf("%s topic-partitions are assigned to %s consumer\n",
                            Arrays.toString(partitions.toArray()), name);
                    consumer.seekToEnd(partitions);
                }
            });

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                if (records.count() > 0) {
                    consumer.commitAsync();
                }
                for (ConsumerRecord<String, byte[]> record : records) {

                    System.out.printf(
                            this.name
                                    + " -----> Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(),
                            deserialize(record.value()));

                    System.out.println("\r\n");
                }

            }

        } catch (Exception e) {
            // if (args.length == 0) {
            // parser.printHelp();
            // System.exit(0);
            // } else {
            // parser.handleError(e);
            // System.exit(1);
            // }
        } finally {
            System.out.println("=====================================");
            consumer.close();
        }

    }

    private static Object deserialize(final byte[] objectData) {
        return org.apache.commons.lang3.SerializationUtils.deserialize(objectData);
    }

}