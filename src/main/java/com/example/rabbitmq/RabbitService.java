package com.example.rabbitmq;

import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.server.ServerRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static reactor.rabbitmq.BindingSpecification.binding;

@Service
public class RabbitService {
    private final RabbitConfig rabbitConfig;

    public RabbitService(RabbitConfig rabbitConfig) {
        this.rabbitConfig = rabbitConfig;
    }

    public Mono<String> enqueue(ServerRequest request){
        String srcMRN = request.headers().firstHeader("srcMRN");
        String queue = request.headers().firstHeader("dstMRN")+"::"+srcMRN;

        Flux<OutboundMessage> msg =
                Flux.just(new OutboundMessage("",queue,"TEST".getBytes()));

        rabbitConfig.declare(queue);
        rabbitConfig.sendMessage(msg);

        return Mono.just("OK");
    }

    public Flux<String> consume(ServerRequest request) {
        String srcMRN = request.headers().firstHeader("srcMRN");
        String queue = request.headers().firstHeader("dstMRN")+"::"+srcMRN;

        rabbitConfig.declare(queue);

        return rabbitConfig.receiver().consumeAutoAck(queue).log("long-polling")
                .flatMap(m->Mono.just(new String(m.getBody())));
    }

}
