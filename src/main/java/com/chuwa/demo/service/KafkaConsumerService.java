package com.chuwa.demo.service;


import com.chuwa.demo.dao.MessageRepository;
import com.chuwa.demo.entity.MessageEntity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;


@Service
public class KafkaConsumerService {
    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;
    @Value("${kafka.topic.name}")
    private String topic;

    @Autowired
    private MessageRepository repository;
    //How do kafka consumers "consume" messages from broker
    //Kafka consumer: poll
    //Read the topic-partion on assigned broker, by offset

    //What will Kafka consumer do when above operation failed
    //If above operation fails: indicate assigned broker is down
    //The consumer will read from the new leader

    // At-Least-Once
    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void listenGroupFoo(ConsumerRecord<String, String> record, Acknowledgment ack) {
        System.out.printf("Received message: key = %s, value = %s from group: %s with topic: %s",
                record.key(), record.value(), consumerGroupId, topic);
        try {
            // process the message: save to database
            MessageEntity message = new MessageEntity();
            message.setMessageKey(record.key());
            message.setMessageValue(record.value());
            repository.save(message);
            // explicitly commit the offset of the processed message
            ack.acknowledge();
        } catch (Exception e) {
            System.err.println("Failed to process message: " + e.getMessage());
            // if no successful processing, kafka will retry the message
        }
    }
    // At-Most-Once
//    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
//    public void listenGroupFoo(ConsumerRecord<String, String> record, Acknowledgment ack) {
//        ack.acknowledge(); // commit first
//        System.out.printf("Received message: key = %s, value = %s from group: %s with topic: %s",
//                record.key(), record.value(), consumerGroupId, topic);
//        try {
//            // process the message: save to database
//            MessageEntity message = new MessageEntity();
//            message.setMessageKey(record.key());
//            message.setMessageValue(record.value());
////            if (record.value().contains("fail")) {
////                throw new RuntimeException("Simulation processing failure");
////            }
//            repository.save(message);
//        } catch (Exception e) {
//            System.err.println("Failed to process message: " + e.getMessage());
//        }
//    }

}
