package com.github.serdardundar.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerDemoWithThread {

    private ConsumerDemoWithThread() {
    }

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        log.info("Creating the consumer");

        // latch for dealing multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        ConsumerRunnable consumerRunnable = new ConsumerRunnable("first_topic", latch);

        // start the thread
        Thread myThread = new Thread(consumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            consumerRunnable.shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has exited");
        }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrrupted", e);
        } finally {
            log.info("Application is closing");
        }
    }

    @Slf4j
    public static class ConsumerRunnable implements Runnable {

        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String topic, CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-fifth-kafka-app");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //create consumer
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                //poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Key: " + record.key() + ", Value:" + record.value());
                        log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException exception) {
                log.info("Received shutdown signal!");
            } finally {
                consumer.close();
                //we're done with consumer
                latch.countDown();
            }
        }

        public void shutDown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            consumer.wakeup();
        }
    }
}
