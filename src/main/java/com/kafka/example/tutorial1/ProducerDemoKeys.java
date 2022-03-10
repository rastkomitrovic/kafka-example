package com.kafka.example.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        //create producer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //send data


        // asynchronous
        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "Hello " + i;
            String key = "key_id_ " + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            logger.info("\nKey:" + key);
            //id 0 to partition 2
            //id 1 to partition 1
            //id 2 partition 0

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes everythime successfully sent or exception thrown

                    if (e == null) {
                        logger.info("\nReceived new metadata. \nTopic:" + recordMetadata.topic() + "\nPartition:" + recordMetadata.partition() + "\nOffset:" + recordMetadata.offset() + "\nTimestamp:" + recordMetadata.timestamp() + "\nMessage:" + record.value() + "\n");
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //.get() makes it synchronous - dont do it on prod, blocks send
            producer.flush();
        }

        producer.close();
    }
}
