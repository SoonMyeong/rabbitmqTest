package com.example.rabbitmq;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

@Configuration
public class Router {
    @Bean
    public RouterFunction<ServerResponse> routes(Handler handler){
        return RouterFunctions.route()
                .POST("/test",handler::test)
                .POST("/consume",handler::consume)
                .build();
    }
}
