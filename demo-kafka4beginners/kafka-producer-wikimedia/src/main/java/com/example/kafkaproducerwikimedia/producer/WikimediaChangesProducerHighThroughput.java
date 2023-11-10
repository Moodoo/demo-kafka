package com.example.kafkaproducerwikimedia.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducerHighThroughput {
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

        //high troughput producer
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024));
        kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                "snappy");
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
