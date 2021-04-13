package com.example.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Component
public class RabbitConfig {
    private Mono<? extends Connection> connectionMono;
    private Mono<com.rabbitmq.client.Connection> declareMono;
    private ChannelPool channelPool;
    private ResourceManagementOptions resourceManagementOptions;

    /**
     * Reactor RabbitMQ <br>
     * ConnectionFactory : connection 관련 정보 설정 <br>
     * connectionMono : connection 하나로 receiver instance 와 sender instance 를 같이 쓰기 위한 connection <br>
     * channelPool : sender 사용 시 (메시지 전송) 무한히 늘어나는 채널 대신 재 사용하는 Pool <br>
     * declareMono : Queue declare 시 사용하는 connection <br>
     * channelMono : declare 사용 시 채널을 사용하게 되는데 이때 특정 채널 한개만 캐시하여 사용하도록 하기 위한 캐시 channel <br>
     * queueSpecification : queue declare 시 사용하는 옵션, durable 속성 적용 위해 사용  <br>
     * 자세한 내용은  https://projectreactor.io/docs/rabbitmq/release/reference/#_introduction 을 참고 <br>
     *
     * 커넥션과 채널 모두 유지 및 재 활용 하므로 close 하지 않음. (Application 종료 시 해제 됨) <br>
     *
     *  해당 relaying 서비스에서는 channel Pool 을 사용 하며 큐 선언 시에는 channelMono (캐시) 를 사용 한다. <br>
     *
     *  1개의 커넥션을 유지 하면서 메시지 send 시 Channel Pool 을 사용 한다. ( Channel 의 경우 서버 CPU 와 메모리 성능에 따라 간다 ) <br>
     *
     */
    @PostConstruct
    void init() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("");
        connectionFactory.setPassword("");

        this.connectionMono
                = Utils.singleConnectionMono(connectionFactory, cf->cf.newConnection("enqueue"));

        this.channelPool = ChannelPoolFactory.createChannelPool(
                this.connectionMono,
                new ChannelPoolOptions().maxCacheSize(10)
        );


        try {
            declareMono = Mono.just(connectionFactory.newConnection("relaying-declare"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        Mono<Channel> channelMono = declareMono.map(c -> {
            try {
                return c.createChannel();
            } catch (Exception e) {
                throw new RabbitFluxException(e);
            }
        }).cache();

        this.resourceManagementOptions
                = new ResourceManagementOptions().channelMono(channelMono);

        QueueSpecification queueSpecification = new QueueSpecification();
        queueSpecification.durable(true);

    }

    public ChannelPool getChannelPool(){
        return this.channelPool;
    }

    private Mono<? extends Connection> getConnectionMono(){
        return this.connectionMono;
    }
    @PostConstruct
    public Sender sender(){
        return RabbitFlux.createSender(new SenderOptions().connectionMono(getConnectionMono()));
    }

    public ResourceManagementOptions getResourceManagementOptions() {return this.resourceManagementOptions; }

    /**
     * queue 생성 하는 메소드 <br>
     * 요청 마다 생성 되는 큐가 다를 수 있음을 고려 함 <br>
     * @param queue
     * @return
     */
    public void declare(String queue) {
        Sender sender = RabbitFlux.createSender(new SenderOptions().connectionMono(declareMono));
        Mono<Channel> channelMono = this.declareMono.map(c -> {
            try {
                return c.createChannel();
            } catch (Exception e) {
                throw new RabbitFluxException(e);
            }
        }).cache();

        this.resourceManagementOptions
                = new ResourceManagementOptions().channelMono(channelMono);

        sender.declareQueue(QueueSpecification.queue(queue),getResourceManagementOptions()).log("declare")
                .subscribe(declareOk -> {
                    channelMono.subscribe(channel -> {
                        try {
                            channel.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (TimeoutException e) {
                            e.printStackTrace();
                        }
                    });
                    sender.close();
                });

    }


    public void sendMessage(Flux<OutboundMessage> msg){
        Sender sender = RabbitFlux.createSender(new SenderOptions().connectionMono(getConnectionMono()));
        Mono<? extends Channel> channelMono = this.channelPool.getChannelMono();
        sender.send(msg,new SendOptions().channelMono(channelMono)).log("send")
                .subscribe(unused -> {
                    channelMono.subscribe(channel -> {
                        try {
                            channel.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (TimeoutException e) {
                            e.printStackTrace();
                        }
                    });
                    sender.close();
                });

    }
}
