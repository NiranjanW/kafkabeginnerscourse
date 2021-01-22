package com.github.niran.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConusmerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConusmerDemoAssignSeek.class.getName());

        Properties properties = new Properties();
        String bootstrapServers="127.0.0.1:9092";
        String groupId ="my-fourth-application";
        String topic ="first_topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create Con

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe
//        assign and seek to replay data
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign((Arrays.asList(partitionToReadFrom)));

        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading =true;
        int numberOfMessagesReadSoFar = 0;

        //poll
        while(true) {
            ConsumerRecords<String,String> records =
            consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String ,String> record:records){
                numberOfMessagesToRead +=1;
                logger.info ("Key" + record.key() +  " , Value" + record.value() + "\n" +
                        "Offset" + record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading =false;
                    break;
                }
            }
        }
    }
}
