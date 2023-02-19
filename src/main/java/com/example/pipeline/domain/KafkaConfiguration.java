package com.example.pipeline.domain;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfiguration {

    public final static String EXAMPLE_TOPIC = "example-topic-v3";

    @Bean
    NewTopic exampleTopic() {
        return TopicBuilder.name(EXAMPLE_TOPIC)
                .partitions(12)
                .build();
    }
}
