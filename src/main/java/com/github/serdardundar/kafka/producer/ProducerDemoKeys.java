package com.github.serdardundar.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 100; i++) {

            //create a producer record
            String topic = "first_topic";
            String value = "hello world " + i;
            String key = "id_" + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            log.info("Key: " + key);
            //send data - async
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    //the record sent successfully
                    log.info("\n" +
                            "Received new metadata --> \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + " \n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    log.error("Error while producing", e);
                }
            }).get(); // don't use get in prod
        }
        producer.close();
    }
}
