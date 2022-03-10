package com.kafka.example.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //latest or none, earliest read from begining, latest from only new messages, none will throw error if no offsets are saved

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //assign and seek are mostly used to replay data or fetch a specific message

        //assign
        TopicPartition partition = new TopicPartition("first_topic",0);
        consumer.assign(Arrays.asList(partition));
        long offsetToReadFrom = 15L;

        //seek

        consumer.seek(partition,offsetToReadFrom);
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesRead = 0;
        //poll data

        while(keepOnReading){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0

            for(ConsumerRecord<String,String> record: records){
                numberOfMessagesRead ++;
                logger.info("\nKey: "+ record.key()+", Value: "+record.value()+", Partition: "+record.partition()+", Offset: "+ record.offset());
                if(numberOfMessagesRead >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

        logger.info("Exiting Application");
    }
}
