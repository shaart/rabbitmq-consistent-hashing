package org.example.consumerapp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.CustomExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@EnableRabbit
public class RabbitConfig {

    public static final String EXCHANGE_TYPE_CONSISTENT_HASH = "x-consistent-hash";
    public static final String SPACE_EVENTS_TO_ROLES_PARTITION_1_QUEUE = "space.events.to.roles.partition.1.q";
    public static final String SPACE_EVENTS_TO_ROLES_PARTITION_2_QUEUE = "space.events.to.roles.partition.2.q";
    public static final String SPACE_EVENTS_EXCHANGE = "space.events";

    private static boolean isExchangeExists(RabbitAdmin rabbitAdmin, String exchange) {
        return Boolean.TRUE.equals(rabbitAdmin.getRabbitTemplate().execute(channel -> {
            try {
                channel.exchangeDeclarePassive(exchange);
                return true;
            } catch (Exception e) {
                return false;
            }
        }));
    }

    private Binding bindConsistentHashingExchangeWithQueue(Queue queue, Exchange exchange) {
        String queueWeight = "1";
        log.info("Bind queue '{}' to exchange '{}'", queue.getName(), exchange.getName());
        return BindingBuilder.bind(queue)
                .to(exchange)
                .with(queueWeight)
                .noargs();
    }

    private Queue createDurableSingleActiveConsumerQueue(String name) {
        log.info("Created durable queue with name {}", name);
        return new Queue(name, true, false, false);
    }

    private CustomExchange createDurableConsistentHashExchange(String name) {
        log.info("Created durable consistent-hash exchange with name {}", name);
        return new CustomExchange(name, EXCHANGE_TYPE_CONSISTENT_HASH, true, false);
    }

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    public Exchange spaceEventsToRolesExchange() {
        return createDurableConsistentHashExchange("space.events.to.roles");
    }

    @Bean
    public Queue spaceEventsToRolesPartition1() {
        return createDurableSingleActiveConsumerQueue(SPACE_EVENTS_TO_ROLES_PARTITION_1_QUEUE);
    }

    @Bean
    public Queue spaceEventsToRolesPartition2() {
        return createDurableSingleActiveConsumerQueue(SPACE_EVENTS_TO_ROLES_PARTITION_2_QUEUE);
    }

    @Bean
    public Binding spaceEventsToRolesPartition1Binding(
            @Qualifier("spaceEventsToRolesPartition1") Queue queue,
            @Qualifier("spaceEventsToRolesExchange") Exchange exchange) {
        return bindConsistentHashingExchangeWithQueue(queue, exchange);
    }

    @Bean
    public Binding spaceEventsToRolesPartition2Binding(
            @Qualifier("spaceEventsToRolesPartition2") Queue queue,
            @Qualifier("spaceEventsToRolesExchange") Exchange exchange) {
        return bindConsistentHashingExchangeWithQueue(queue, exchange);
    }

    @Bean
    public Binding spaceEventsExchangeBindToSpaceEventsToRolesExchange(
            @Qualifier("spaceEventsToRolesExchange") Exchange spaceEventsToRolesExchange,
            RabbitAdmin rabbitAdmin) {
        log.info("Trying to bind exchange '{}' to exchange '{}'", spaceEventsToRolesExchange.getName(), SPACE_EVENTS_EXCHANGE);
        if (!isExchangeExists(rabbitAdmin, SPACE_EVENTS_EXCHANGE)) {
            throw new IllegalStateException("Exchange '%s' does not exist in RabbitMQ!".formatted(SPACE_EVENTS_EXCHANGE));
        }

        // TODO почему не создаётся этот бинд автоматически?
        return BindingBuilder.bind(spaceEventsToRolesExchange)
                .to(new FanoutExchange(SPACE_EVENTS_EXCHANGE));
    }
}
