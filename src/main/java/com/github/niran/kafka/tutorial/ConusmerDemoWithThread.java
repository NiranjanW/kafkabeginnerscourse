package com.github.niran.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConusmerDemoWithThread {
    public static void main(String[] args) {

        new ConusmerDemoWithThread().run();
    }

    private ConusmerDemoWithThread() {

    }

    private void run() {

        Logger logger = LoggerFactory.getLogger(ConusmerDemoWithThread.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //Create Consumer Runnable
        logger.info("Creating Consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        //start Thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("App interrupted" + e);
        } finally {
            logger.info("App is exited");
        }

    }


    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            this.latch = latch;
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));

        }


        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key" + record.key() + " , Value" + record.value() + "\n" +
                                "Offset" + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Rceived Shutdown Signal!");
            } finally {
                consumer.close();
                //tell main code we are done with consumer , shutdown
                latch.countDown();
            }
        }

        public void shutdown() {
            //the wakeup() is a special method to interrupt consumer.poll()
            //it will throw wakeup exception
            consumer.wakeup();
        }
    }
}
