package com.chuwa.demo.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    public String bootstrapServers;

//    @Value("${spring.kafka.consumer.group-id}")
//    public String consumerGroupId;

    @Value("${kafka.topic.name}")
    public String topic;

    // Helper method to create consumer properties， using groupId as param for each consumer group
    private Map<String, Object> consumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // At-most-once config
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // default is true
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000); // automatically commit its current offset to Kafka every 1000 milliseconds
        // At-least-once config
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }


    // Create multiple consumer groups
    @Bean
    public ConsumerFactory<String, String> consumerFactory1() {
        return new DefaultKafkaConsumerFactory<>(consumerProps("consumer_group_1"));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    kafkaListenerContainerFactory1() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory1());
        factory.setConcurrency(3);
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);  // At-least-once: required for manual acknowledgment
        return factory;

    }

//    @Bean
//    public ConsumerFactory<String, String> consumerFactory2() {
//        return new DefaultKafkaConsumerFactory<>(consumerProps("consumer_group_2"));
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String>
//    kafkaListenerContainerFactory2() {
//
//        ConcurrentKafkaListenerContainerFactory<String, String> factory =
//                new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory2());
//        factory.setConcurrency(4);
//        return factory;
//    }
//
//    @Bean
//    public ConsumerFactory<String, String> consumerFactory3() {
//        return new DefaultKafkaConsumerFactory<>(consumerProps("consumer_group_3"));
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String>
//    kafkaListenerContainerFactory3() {
//
//        ConcurrentKafkaListenerContainerFactory<String, String> factory =
//                new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory3());
//        factory.setConcurrency(2);
//        return factory;
//    }
}
