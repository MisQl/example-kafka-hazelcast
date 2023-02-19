package com.example.pipeline.domain;

import com.example.pipeline.model.ExampleEventV1;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.example.pipeline.domain.KafkaConfiguration.EXAMPLE_TOPIC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Component
@RequiredArgsConstructor
public class EventPublisher {

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss:SSS");
    private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedDelay = 100, timeUnit = MILLISECONDS)
    void eventProducer() throws InterruptedException {
        var key = UUID.randomUUID().toString();
        var event = new ExampleEventV1(TIME_FORMATTER.format(LocalTime.now()));
        kafkaTemplate.send(EXAMPLE_TOPIC, key, event);
        MILLISECONDS.sleep(RANDOM.nextInt(100, 300));
    }
}
