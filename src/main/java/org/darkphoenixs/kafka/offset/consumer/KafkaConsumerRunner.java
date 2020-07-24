/*
 * Copyright (c) 2018. Dark Phoenixs (Open-Source Organization).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.darkphoenixs.kafka.offset.consumer;

import kafka.coordinator.group.GroupMetadataManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaConsumerRunner implements Runnable {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer consumer;
    private final String topic;
    private ConcurrentMap<String, AtomicInteger> count = new ConcurrentHashMap<>();
    private volatile long startTime, stopTime;

    public KafkaConsumerRunner(Properties props, String topic) {
        this.consumer = new KafkaConsumer(props);
        this.topic = topic;
    }

    public void run() {
        try {
            consumer.subscribe(Arrays.asList(topic));
            startTime = System.currentTimeMillis();
            while (!closed.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    receive(GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key())).toString());
                }
            }

        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    public void receive(String message) {

        String[] messages = message.replace("[", "").replace("]", "").split(",");

        if (messages.length > 1) {
            String key = messages[0] + "::" + messages[1];
            if (count.containsKey(key)) {
                count.get(key).incrementAndGet();
            } else {
                AtomicInteger i = new AtomicInteger(1);
                count.putIfAbsent(key, i);
            }
        }
    }

    private void print() {

        System.out.println("-------------------");

        List<Map.Entry<String, AtomicInteger>> entryList = new ArrayList<>(count.entrySet());
        Collections.sort(entryList, (o1, o2) -> o2.getValue().get() - o1.getValue().get());

        Iterator<Map.Entry<String, AtomicInteger>> iter = entryList.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, AtomicInteger> entry = iter.next();
            System.out.println(entry.getKey() + " --> " + entry.getValue());
        }

        System.out.println("-------------------");
        System.out.println("acquisition time: " + (stopTime - startTime) / 1000 + " s.");
        System.out.println("-------------------");
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        stopTime = System.currentTimeMillis();
        print();
        closed.set(true);
        consumer.wakeup();
    }

}
