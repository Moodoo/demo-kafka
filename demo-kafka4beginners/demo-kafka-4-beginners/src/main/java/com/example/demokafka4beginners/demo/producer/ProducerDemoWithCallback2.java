package com.example.demokafka4beginners.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

@Slf4j
public class ProducerDemoWithCallback2 {
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

        kafkaProps.put("batch.size",400);
       // kafkaProps.put("partitioner.class",RoundRobinPartitioner.class.getName());

        //Create a Kafka producer from configuration
        KafkaProducer<String, String> simpleProducer = new KafkaProducer<>(kafkaProps);

        // create producer record
        for (int j = 0; j < 10; ++j)
        {
            for (int i = 0; i < 10; ++i) {
                ProducerRecord<String, String> kafkaRecord =
                        new ProducerRecord<String, String>(
                                "java.demo.topic",    //Topic name
                                String.valueOf(i),          //Key for the message
                                "This is order : " + i       //Message Content
                        );

                simpleProducer.send(kafkaRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executed on exception or succesfullt send
                        if (e == null) {
                            log.info("Received metadata" + "topic " + recordMetadata.topic() +
                                    " topic: " + recordMetadata.topic() +
                                    " Partition: " + recordMetadata.partition() +
                                    " Offset: " + recordMetadata.offset() +
                                    " timestamp: " + recordMetadata.timestamp()
                            );
                        } else {
                            log.error(e.getMessage());
                        }
                    }
                });
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        simpleProducer.flush();
        simpleProducer.close();
    }
}
