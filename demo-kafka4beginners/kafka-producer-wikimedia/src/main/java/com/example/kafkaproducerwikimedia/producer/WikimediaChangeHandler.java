package com.example.kafkaproducerwikimedia.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class WikimediaChangeHandler implements EventHandler {
   private KafkaProducer<String,String> kafkaProducer;
   private String topic;


    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen()  {
        //nothin here
    }

    @Override
    public void onClosed()  {
       kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent)  {
         //async
        log.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String s)  {
// nothing here
    }

    @Override
    public void onError(Throwable throwable) {
     log.error("error in stream reading",throwable);
    }
}
