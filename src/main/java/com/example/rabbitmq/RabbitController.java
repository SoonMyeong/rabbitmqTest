package com.example.rabbitmq;

import com.rabbitmq.client.Delivery;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RestController
public class RabbitController {

    private final RabbitService rabbitService;

    public RabbitController(RabbitService rabbitService) {
        this.rabbitService = rabbitService;
    }

    @GetMapping("/consume")
    public Flux<String> consume() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(10);
        Disposable disposable = rabbitService.consume(latch);
        latch.await(1, TimeUnit.SECONDS);
        disposable.dispose();

        return Flux.just("test");
    }
}
