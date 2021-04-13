package com.example.rabbitmq;

import com.rabbitmq.client.Delivery;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
public class RabbitController {

    private final RabbitService rabbitService;

    public RabbitController(RabbitService rabbitService) {
        this.rabbitService = rabbitService;
    }

    @PostMapping("/test")
    public Mono<String> test() {
        return rabbitService.enqueue();
    }

    @GetMapping("/consume")
    public Flux<String> consume() throws InterruptedException, IOException, TimeoutException {
        CountDownLatch latch = new CountDownLatch(10);
        rabbitService.consume(latch);

        return Flux.just("test");
    }
}
