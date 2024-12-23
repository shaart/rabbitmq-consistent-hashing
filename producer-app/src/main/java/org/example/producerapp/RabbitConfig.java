package org.example.producerapp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@EnableRabbit
public class RabbitConfig {

    public static final String SPACE_EVENTS_EXCHANGE = "space.events";

    private Exchange createDurableFanoutExchange(String name) {
        log.info("Created durable fanout exchange with name {}", name);
        return ExchangeBuilder.fanoutExchange(name)
                .durable(true)
                .build();
    }

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    public Exchange spaceEventsExchange() {
        return createDurableFanoutExchange(SPACE_EVENTS_EXCHANGE);
    }
}
