package com.github.niran.kafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class PrducerDemoKeys {


    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        final Logger logger = LoggerFactory.getLogger("ProducerDemo");

        String propFileName = "config.properties";

//        try (InputStream input = ProducerDemo.class.getClassLoader().getResourceAsStream(propFileName)) {
//            Properties prop = new Properties();
//            if (input == null) {
//                System.out.println("Sorry, unable to find config.properties");
//                return;
//            }
//
//            prop.load(input);
//            String bootstrapServers = prop.getProperty("bootStrapServer =127.0.0.1:9092");
//        }catch (IOException ex) {
//                ex.printStackTrace();
//            }

        //load a properties file from class path, inside static method


        String bootstrapServers = "127.0.0.1:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //producer record
        for (int i = 0; i < 10; i++) {

            String topic ="first_topic";
            String value ="hello world" + Integer.toString(i);
            String key ="id_" + Integer.toString(i);



            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic , key,value);
            logger.info("key" + key);
            //Send data -async
            producer.send(record,
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e == null) {
                                logger.info("Recevied new Metadata \n" +
                                        "Topic" + recordMetadata.topic() + "\n" +
                                        "Partition " + recordMetadata.partition() + "\n" +
                                        "Offset " + recordMetadata.offset() + "\n" +
                                        "TimeStamp " + recordMetadata.timestamp()
                                );
                            } else {
                                logger.error("Error while producing" + e);
                            }
                        }

                    //block send;
                    }).get() ;
        }
            producer.flush();
            producer.close();

        }
    
   };