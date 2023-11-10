package com.example.demokafka4beginners.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

@Slf4j
public class ProducerDemoWithKeys {
    public static void main(String[] args) {
        //Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();

        //List of brokers to connect to
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        //Serializer class used to convert Keys to Byte Arrays
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        //Serializer class used to convert Messages to Byte Arrays
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        //Create a Kafka producer from configuration
        KafkaProducer<String, String> simpleProducer = new KafkaProducer<>(kafkaProps);

        // create producer record
        for (int j = 0; j < 2; ++j) {
            for (int i = 0; i < 10; ++i) {
                String key = "id_" + i;
                ProducerRecord<String, String> kafkaRecord =
                        new ProducerRecord<String, String>(
                                "java.demo.topic",    //Topic name
                                key,          //Key for the message
                                "This is order : " + i       //Message Content
                        );


                simpleProducer.send(kafkaRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executed on exception or succesfullt send
                        if (e == null) {
                            log.info("Received metadata\n" + "topic " + recordMetadata.topic() +
                                    " key: " + String.valueOf(key) +
                                  //  " topic: " + recordMetadata.topic() +
                                    " Partition: " + recordMetadata.partition()
                                  // + " Offset: " + recordMetadata.offset() +
                                  //  " timestamp: " + recordMetadata.timestamp()
                            );
                        } else {
                            log.error(e.getMessage());
                        }
                    }
                });
            }
        }
        simpleProducer.flush();
        simpleProducer.close();
    }
}
