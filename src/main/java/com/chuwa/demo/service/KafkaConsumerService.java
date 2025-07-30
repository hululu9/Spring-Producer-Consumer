package com.chuwa.demo.service;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;


@Service
public class KafkaConsumerService {
//    @Value("${spring.kafka.consumer.group-id}")
//    private String consumerGroupId;
    @Value("${kafka.topic.name}")
    private String topic;

    //How do kafka consumers "consume" messages from broker
    //Kafka consumer: poll
    //Read the topic-partion on assigned broker, by offset

    //What will Kafka consumer do when above operation failed
    //If above operation fails: indicate assigned broker is down
    //The consumer will read from the new leader
    @KafkaListener(topics = "${kafka.topic.name}", groupId = "consumer_group_1", containerFactory = "kafkaListenerContainerFactory1")
    public void listenGroup1(String message) {
        System.out.println("Received message: " + message + " from group: consumer_group_1" + " with topic: " + topic);
        // {"messageKey":1234 //idempotency key
        // }
//        try {
//            //process and persist message
//        } catch (Exception e) {
//            //send to dead letter queue
//        }
        //commit to offset here
        //return
    }
      // Multiple consumer groups
//    @KafkaListener(topics = "${kafka.topic.name}", groupId = "consumer_group_2", containerFactory = "kafkaListenerContainerFactory2")
//    public void listenGroup2(String message) {
//        System.out.println("Received message: " + message + " from group: consumer_group_2" + " with topic: " + topic);
//    }
//
//    @KafkaListener(topics = "${kafka.topic.name}", groupId = "consumer_group_3", containerFactory = "kafkaListenerContainerFactory3")
//    public void listenGroup3(String message) {
//        System.out.println("Received message: " + message + " from group: consumer_group_3" + " with topic: " + topic);
//    }

      // At-least-once
//    @KafkaListener(topics = "${kafka.topic.name}", groupId = "consumer_group_1", containerFactory = "kafkaListenerContainerFactory1")
//    public void listenGroup1(ConsumerRecord<String, String> record, Acknowledgment ack) {
//        String message = record.value();
//        long offset = record.offset();
//        int partition = record.partition();
//        String key = record.key();
//
//        System.out.printf("Received message: %s | Key: %s | Partition: %d | Offset: %d%n",
//                message, key, partition, offset);
//        // Simulate failure of processing
//        if (message.contains("fail")) {
//            System.err.println("Simulated failure, throwing exception...");
//            throw new RuntimeException("Simulated processing error");
//        }
//        // Only commit offset after successful processing
//        ack.acknowledge();
//        System.out.printf("Acknowledged offset: %d from partition %d%n", offset, partition);
//    }

}
