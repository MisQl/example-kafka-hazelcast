package com.example.pipeline.domain;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static com.example.pipeline.domain.KafkaConfiguration.EXAMPLE_TOPIC;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

@Configuration
@RequiredArgsConstructor
public class HazelcastPipeline implements CommandLineRunner {

    private static final String JET_JOB = "kafka-traffic-monitor";
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss:SSS");

    private final HazelcastInstance hazelcastInstance;
    private final KafkaProperties kafkaProperties;

    // https://hazelcast.com/blog/streaming-with-spring/

    @Override
    public void run(String... args) {
        var jetService = hazelcastInstance.getJet();
        var pipeline = createPipeline();
        var jobConfig = new JobConfig().setName(JET_JOB);
        jetService.newJob(pipeline, jobConfig);
    }

    private Pipeline createPipeline() {
        var pipeline = Pipeline.create();
        var properties = properties();
        pipeline.readFrom(KafkaSources.kafka(properties, EXAMPLE_TOPIC))
                .withNativeTimestamps(0)
                .window(sliding(1000, 500))
                .aggregate(counting())
                .writeTo(Sinks.logger(wr -> String.format("At %s Kafka got %,d tweets per second", TIME_FORMATTER.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(wr.end()), ZoneId.systemDefault())), wr.result())));
        return pipeline;
    }

    private Properties properties() {
        var properties = new Properties();
        var consumerProperties = kafkaProperties.buildConsumerProperties();
        properties.putAll(consumerProperties);
        return properties;
    }
}
