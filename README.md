1. Run rabbit
```shell
docker-compose up -d rabbitmq
```

2. Create a bind from fanout exchange `space.events` to consistent-hash exchange `space.events.to.roles`, 
because by issue it won't created by `org.example.consumerapp.RabbitConfig.spaceEventsExchangeBindToSpaceEventsToRolesExchange`.

3. Run `consumer-app` changing `SERVER_PORT` to needed.
4. Run `producer-app` changing `SERVER_PORT` to needed.