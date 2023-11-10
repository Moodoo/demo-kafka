package com.example.demokafka4beginners.demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerDemoCooperative {
    public static void main(String[] args) {
        //Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();

        //List of brokers to connect to
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        //Serializer class used to convert Keys to Byte Arrays
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        //Serializer class used to convert Messages to Byte Arrays
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        //Consumer Group ID for this consumer
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,
                "java-consumer-demo");

        //Set to consume from the earliest message, on start when no offset is
        //available in Kafka//none/earliest/latest
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");

        kafkaProps.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
 //kafkaProps.put("group.instance.id","??"); strategy for static assignments
        //Create a Consumer
        KafkaConsumer<String, String> simpleConsumer =
                new KafkaConsumer<String, String>(kafkaProps);


        final Thread mainThreas = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("DetectedShutdown, exit  wby calling consumer wakeup()");
                simpleConsumer.wakeup();
                //join main thread to allow execution of the cod to the main thread
                try {
                    mainThreas.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        //Subscribe to the kafka.learning.orders topic
        try {
            simpleConsumer.subscribe(Arrays.asList("java.demo.topic"));


            //Continuously poll for new messages
            while (true) {

                //Poll with timeout of 100 milli seconds
                ConsumerRecords<String, String> messages =
                        simpleConsumer.poll(Duration.ofMillis(100));//wakeup exception

                //Print batch of records consumed
                for (ConsumerRecord<String, String> message : messages) {
                    log.info("key" + message.key() + ", value: " + message.value());
                    log.info("partition" + message.partition() + ", offset: " + message.offset());
                }

            }
        } catch (WakeupException e) {
            log.info("consumer is about to shut down");
        }
        catch(Exception e)
        {
            log.error("unexpected exception", e);
        }
        finally
        {
            simpleConsumer.close();
            log.info("consumer is  shut down");
        }


    }
}
