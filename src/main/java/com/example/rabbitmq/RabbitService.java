package com.example.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

@Service
public class RabbitService {


    public Disposable consume(CountDownLatch latch){
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        connectionFactory.setUsername("mcp");
        connectionFactory.setPassword("mcp");

        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(connectionFactory)
                .connectionSubscriptionScheduler(Schedulers.boundedElastic());

       Receiver receiver = RabbitFlux.createReceiver(receiverOptions);

      return receiver.consumeAutoAck("MCPTOSV00").log()
                                .subscribe(m->{
                                    System.out.println(new String(m.getBody()));
                                    latch.countDown();
                                });

    }

}
