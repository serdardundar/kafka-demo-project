package com.github.serdardundar.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

@Slf4j
public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            //create a producer record
            ProducerRecord<String, String> recordForJava = new ProducerRecord<>("demo_java", "hello java " + i);

            //send data - async
            producer.send(recordForJava, (recordMetadata, e) -> {
                if (e == null) {
                    //the record sent successfully
                    log.info("\n" +
                        "Received new metadata/ \n" +
                        "Topic: " + recordMetadata.topic() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + " \n" +
                        "Date: " + new Date(recordMetadata.timestamp()));
                } else {
                    log.error("Error while producing", e);
                }
            });

            // this is added to prove to use the RoundRobinPartitioner implementation
            // if you remove this try-catch, it will be StickyPartitioner implementation
            /*   try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
        }
        producer.close();
    }
}
