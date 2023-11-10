package com.example.kafkaproducerwikimedia.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducerSafeProducer {
    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer = "localhost:9092";

        //Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServer);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        //safe producer by default for kafka >3.0
        kafkaProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,   true);
        kafkaProps.put(ProducerConfig.ACKS_CONFIG,  String.valueOf(-1)); //all
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG,String.valueOf(Integer.MAX_VALUE));

        KafkaProducer<String, String> simpleProducer = new KafkaProducer<>(kafkaProps);


        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(simpleProducer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        //start producer in anothe thread
        eventSource.start();

        //produce for 10 min and blok
        TimeUnit.MINUTES.sleep(10);
    }
}
