package com.example.kadkaconsumeropensearch.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Collections;
import java.util.Properties;


@Slf4j
public class OpenSeachConsumerBatchData {
    public static void main(String[] args) {
        //connect to openseach
        // find code here

        // CreateIndex
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        String topic = "wikimedia.recentchange";
        kafkaConsumer.subscribe(Collections.singleton(topic));
        try (kafkaConsumer) {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(3000);
                int recordCounts = records.count();
                log.info("record counts: " + recordCounts);

                for (ConsumerRecord<String, String> record : records) {
                    //send data

                    //idepotent
                    // stratedy 1  define ID using kadka coordinates and add to the indexed record
                    String id=record.topic()+"_"+record.partition()+"_"+record.offset();
                    // strategy- extract id from record if is unique


                }
            }
        }
        catch(Exception e)
        {
            log.error("",e);
        }

    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties kafkaProps = new Properties();

        //List of brokers to connect to
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //Serializer class used to convert Keys to Byte Arrays
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        //Serializer class used to convert Messages to Byte Arrays
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        //Consumer Group ID for this consumer
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-opensearch-demo");

        //Set to consume from the earliest message, on start when no offset is
        //available in Kafka//none/earliest/latest

        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //Create a Consumer
        KafkaConsumer<String, String> simpleConsumer =
                new KafkaConsumer<String, String>(kafkaProps);

         return  simpleConsumer;
    }
}
