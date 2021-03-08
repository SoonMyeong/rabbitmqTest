package com.example.rabbitmq;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RestController
public class RabbitController {

    private final RabbitService rabbitService;

    public RabbitController(RabbitService rabbitService) {
        this.rabbitService = rabbitService;
    }

    @GetMapping("/consume")
    public String consume() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(10);
        StringBuilder stringBuilder = new StringBuilder();
        Disposable disposable = rabbitService.consume(latch,stringBuilder);
        latch.await(1, TimeUnit.SECONDS);
        disposable.dispose();

        if(stringBuilder.length()==0){
            return "Empty Queue";
        }
        return stringBuilder.toString();
    }

    @GetMapping("/long-polling")
    public Flux<String> longConsume() throws InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        return rabbitService.longConsume(stringBuilder);
    }

}
