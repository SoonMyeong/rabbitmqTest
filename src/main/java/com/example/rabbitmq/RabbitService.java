package com.example.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.util.concurrent.CountDownLatch;

@Service
public class RabbitService {

    Disposable consume(CountDownLatch latch, StringBuilder abc){
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();

        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(connectionFactory)
                .connectionSubscriptionScheduler(Schedulers.boundedElastic());

        Receiver receiver = RabbitFlux.createReceiver(receiverOptions);

       return receiver.consumeAutoAck("MCPTOSV00").log()
               .flatMap(m-> Mono.just(new String(m.getBody())))
               .subscribe(msg->{
                   abc.append(msg+",");
                   latch.countDown();
               });
    }

    Flux<String> longConsume( StringBuilder abc){
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();

        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(connectionFactory)
                .connectionSubscriptionScheduler(Schedulers.boundedElastic());

        Receiver receiver = RabbitFlux.createReceiver(receiverOptions);

        return receiver.consumeAutoAck("MCPTOSV00").log()
                .flatMap(m-> Mono.just(new String(m.getBody())+","));

    }

}
