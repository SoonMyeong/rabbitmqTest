package com.example.rabbitmq;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

@Component
public class Handler {
    private final RabbitService rabbitService;
    private final RabbitConfig rabbitConfig;

    public Handler(RabbitService rabbitService, RabbitConfig rabbitConfig) {
        this.rabbitService = rabbitService;
        this.rabbitConfig = rabbitConfig;
    }

    Mono<ServerResponse> test(ServerRequest request) {
        return rabbitService.enqueue(request)
                .flatMap(res->ServerResponse.ok().bodyValue(res));
    }

    Mono<ServerResponse> consume(ServerRequest request) {
        return ServerResponse.ok()
                .body(rabbitService.consume(request).next(),String.class);

    }


}
