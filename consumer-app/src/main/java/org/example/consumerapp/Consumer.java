package org.example.consumerapp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import static org.example.consumerapp.RabbitConfig.SPACE_EVENTS_TO_ROLES_PARTITION_1_QUEUE;
import static org.example.consumerapp.RabbitConfig.SPACE_EVENTS_TO_ROLES_PARTITION_2_QUEUE;

@Slf4j
@Component
@RequiredArgsConstructor
public class Consumer {

    private final ObjectMapper objectMapper;

    @SneakyThrows
    private void process(Message message, String queueName) {
        String json = new String(message.getBody());
        JsonNode jsonNode = objectMapper.readTree(json);
        String consumerQueue = message.getMessageProperties().getConsumerQueue();
        log.info("Received from {}: spaceId={}, headers={}",
                consumerQueue, jsonNode.get("spaceId"), message.getMessageProperties().getHeaders());
        Thread.sleep(500L);
    }

    @RabbitListener(queues = SPACE_EVENTS_TO_ROLES_PARTITION_1_QUEUE)
    public void receiveSpaceEventsPartition1(Message message) {
        process(message, SPACE_EVENTS_TO_ROLES_PARTITION_1_QUEUE);
    }

    @RabbitListener(queues = SPACE_EVENTS_TO_ROLES_PARTITION_2_QUEUE)
    public void receiveSpaceEventsPartition2(Message message) {
        process(message, SPACE_EVENTS_TO_ROLES_PARTITION_2_QUEUE);
    }
}
