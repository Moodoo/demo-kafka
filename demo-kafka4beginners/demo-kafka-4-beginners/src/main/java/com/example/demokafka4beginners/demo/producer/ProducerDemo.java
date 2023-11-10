package com.example.demokafka4beginners.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class ProducerDemo {
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
        KafkaProducer<String,String> simpleProducer = new KafkaProducer<>(kafkaProps);

 // create producer record

        ProducerRecord<String,String> kafkaRecord =
                new ProducerRecord<String,String>(
                        "java.demo.topic",    //Topic name
                       "1",          //Key for the message
                        "This is order : " + 1        //Message Content
                );

        simpleProducer.send(kafkaRecord);
        simpleProducer.flush();
        simpleProducer.close();
    }
}
