package org.example.producerapp;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.example.producerapp.RabbitConfig.SPACE_EVENTS_EXCHANGE;

@Slf4j
@RequiredArgsConstructor
@Component
public class Sender implements CommandLineRunner {

    public static final long DELAY_BETWEEN_EVENTS_MS = 100L;
    public static final int INITIAL_SPACE_ID = 1;
    public static final int MAX_SPACE_ID = 1000;
    private static final Map<Integer, Integer> SPACE_ID_TO_COUNT = Stream.iterate(1, i -> i + 1)
            .limit(MAX_SPACE_ID)
            .collect(Collectors.toMap(
                    Function.identity(),
                    v -> ThreadLocalRandom.current().nextInt(1, 10)
            ));
    private final RabbitTemplate rabbitTemplate;
    private final AtomicInteger currentSpaceId = new AtomicInteger(INITIAL_SPACE_ID);
    private final AtomicInteger generatedEventsPerCurrentSpace = new AtomicInteger(0);

    @Override
    public void run(String... args) {
        Stream.iterate(1, i -> i + 1)
                .limit(1_000_000)
                .forEach(eventNum -> {
                    try {
                        generateAndSend(eventNum);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Process was interrupted", e);
                    }
                });
    }

    private void generateAndSend(@NonNull Integer eventNum) throws InterruptedException {
        int spaceId = currentSpaceId.get();
        int generatedEventsCount = generatedEventsPerCurrentSpace.getAndIncrement();
        int neededCount = Optional.ofNullable(SPACE_ID_TO_COUNT.get(spaceId)).orElse(0);
        if (generatedEventsCount > neededCount) {
            spaceId = currentSpaceId.incrementAndGet();
            if (spaceId > MAX_SPACE_ID) {
                currentSpaceId.set(0);
                spaceId = INITIAL_SPACE_ID;
            }
            generatedEventsPerCurrentSpace.set(0);
        }
        UUID eventId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();
        String message = """
                    {
                        "id": "%s",
                        "eventType": "nodeAdded",
                        "spaceId": "%s",
                        "nodeId": "%s",
                        "parentId": null
                    }
                """.formatted(eventId, spaceId, nodeId);
        log.info("Sending event #{} for spaceId={}, eventId={}", eventNum, spaceId, eventId);
        rabbitTemplate.convertAndSend(SPACE_EVENTS_EXCHANGE, "" + spaceId, message);
        Thread.sleep(DELAY_BETWEEN_EVENTS_MS);
    }
}
