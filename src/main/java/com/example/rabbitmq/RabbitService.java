package com.example.rabbitmq;

import org.springframework.stereotype.Service;
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

    public Mono<String> enqueue(){
        String srcMRN = "urn:mrn:neonex:srcMRN";
        String queue = "urn:mrn:neonex:dstMRN"+"::"+srcMRN;

        Flux<OutboundMessage> msg =
                Flux.just(new OutboundMessage("",queue,"TEST".getBytes()));

        rabbitConfig.declare(queue);
        rabbitConfig.sendMessage(msg);

        return Mono.just("OK");
    }

    public void consume(CountDownLatch latch) throws InterruptedException, IOException, TimeoutException {
//        ConnectionFactory connectionFactory = new ConnectionFactory();
//        connectionFactory.useNio();
//        connectionFactory.setHost("127.0.0.1");
//        connectionFactory.setPort(5672);
//        connectionFactory.setUsername("mcp");
//        connectionFactory.setPassword("mcp");
//
//        Connection connection = null;
//        connection = connectionFactory.newConnection();
//        Channel channel = connection.createChannel();
//
//        Mono<Connection> connectionMono = Mono.just(connection);
//        Mono<Channel> channelMono = Mono.just(channel);

//        ReceiverOptions receiverOptions = new ReceiverOptions()
//                .connectionMono(connectionMono);
//        SenderOptions senderOptions = new SenderOptions()
//                .connectionMono(connectionMono)
//                .channelMono(channelMono);
//
//
//        Receiver receiver = RabbitFlux.createReceiver(receiverOptions);
//        Sender sender = RabbitFlux.createSender(senderOptions);
//
//        Flux<OutboundMessage> msg =
//                Flux.just(new OutboundMessage("amq.direct","test","Test".getBytes()));
//
//        sender.declare(QueueSpecification.queue("Test")).log("DECLARE")
//                .then(sender.bind(binding("amq.direct","test","Test")))
//                .subscribe(System.out::println);

//        sender.declareQueue(QueueSpecification.queue("Test").name("Test").durable(true)).log("declare");
//        sender.send(msg).log("Send").subscribe();
//        sender.close();
        //connectionMono.block().close();

//        receiver.consumeAutoAck("Test").log("CONSUME")
//                                .subscribe(m->{
//                                    System.out.println(new String(m.getBody()));
//                                    latch.countDown();
//                                });
//        latch.await(1, TimeUnit.SECONDS);

//        channelMono.block().close();
    }

}
