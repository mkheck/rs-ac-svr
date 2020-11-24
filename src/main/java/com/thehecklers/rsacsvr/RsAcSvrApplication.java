package com.thehecklers.rsacsvr;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.annotation.ConnectMapping;
import org.springframework.stereotype.Controller;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.tools.agent.ReactorDebugAgent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class RsAcSvrApplication {

    public static void main(String[] args) {
        ReactorDebugAgent.init();
        Hooks.onErrorDropped(err -> System.out.println(" Danger, Will Robinson: " + err.getLocalizedMessage()));
        SpringApplication.run(RsAcSvrApplication.class, args);
    }

}

@Configuration
class AircraftServerConfig {
    @Bean
    WebClient client() {
        return WebClient.create("http://localhost:7634/aircraft");
    }
}

@Controller
@RequiredArgsConstructor
class AircraftController {
    @NonNull
    private final WebClient client;

    private final List<RSocketRequester> clients = new ArrayList<>();
    private RSocketRequester requester;

    @ConnectMapping("clients")
    void connectAndRequestStream(RSocketRequester requester,
                                 @Payload String clientId) {
        this.requester = requester;

        requester.rsocket()
                .onClose()
                .doFirst(() -> {
                    System.out.println("Client connected: " + clientId);
                    clients.add(requester);
                })
                .doOnError(e -> System.out.println("Channel closed: " + clientId))
                .doFinally(c -> {
                    clients.remove(requester);
                    System.out.println("Client disconnected: " + clientId);
                })
                .subscribe();

        requester.route("clientwx")
                .data(Mono.just("Need some weather!"))
                .retrieveFlux(Weather.class)
                .subscribe(w -> System.out.println(" 🌬 " + w));

    }

    @MessageMapping("reqresp")
    Mono<Aircraft> requestResponse(Mono<String> timestampMono) {
        return timestampMono.doOnNext(ts -> System.out.println("⏱ " + ts))
                .then(client.get()
                        .retrieve()
                        .bodyToFlux(Aircraft.class)
                        .next());
    }

    @MessageMapping("reqstream")
    Flux<Aircraft> requestStream(Mono<String> timestampMono) {
        return timestampMono.doOnNext(ts -> System.out.println("⏱ " + ts))
                .thenMany(client.get()
                        .retrieve()
                        .bodyToFlux(Aircraft.class));
    }

    @MessageMapping("fireforget")
        //Mono<Void> fireAndForget(Mono<Weather> weatherMono) {
    void fireAndForget(Mono<Weather> weatherMono) {
        weatherMono.subscribe(wx -> System.out.println("☀️ " + wx));
        //return Mono.empty();
    }

    @MessageMapping("channel")
    Flux<Aircraft> channel(Flux<Weather> weatherFlux) {
        return weatherFlux
                .doOnNext(wx -> System.out.println("⛅️ " + wx))
                .switchMap(wx -> client.get()
                        .retrieve()
                        .bodyToFlux(Aircraft.class));
    }
}


@Data
@JsonIgnoreProperties(ignoreUnknown = true)
class Aircraft {
    private String callsign, reg, flightno, type;
    private int altitude, heading, speed;
    private double lat, lon;
}

@Data
@AllArgsConstructor
class Weather {
    private Instant when;
    private String observation;
}