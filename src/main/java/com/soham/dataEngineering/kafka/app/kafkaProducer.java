package com.soham.dataEngineering.kafka.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class kafkaProducer
{
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        //props.put("linger.ms", 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer producer = new KafkaProducer(props) ;

        System.out.println("it has started ");
        for (int i=0 ; i<100 ; i++)
        {
            producer.send(new ProducerRecord("first_topic", "soham is sending messages to Kafka   "+i));
            producer.flush();
        }
        System.out.println("it done ");
        producer.close();
    }
}
