package com.learnreactiveprogramming.streams;

import reactor.core.publisher.Flux;

import java.time.Duration;

public class HotColdPublisherService {

    public static void main(String[] args) throws InterruptedException {
//        Flux<Integer> integerFlux = shareFlux();
//        integerFlux.subscribe(i -> System.out.println("subscriber1: " + i));
//        Thread.sleep(700);
//        integerFlux.subscribe(i -> System.out.println("subscriber2: " + i));
//        Thread.sleep(2000);

//        Flux<Integer> integerFlux = connectFlux();
//        integerFlux.subscribe(i -> System.out.println("subscriber1: " + i));
//        Thread.sleep(700);
//        integerFlux.subscribe(i -> System.out.println("subscriber2: " + i));
//        Thread.sleep(2000);

        Flux<Integer> integerFlux = replayFlux();
        integerFlux.subscribe(i -> System.out.println("subscriber1: " + i));
        Thread.sleep(700);
        integerFlux.subscribe(i -> System.out.println("subscriber2: " + i));
        Thread.sleep(2000);
    }

    public static Flux<Integer> coldFluxDefer() {
        return Flux.defer(() -> Flux.just(1, 2, 3).delayElements(Duration.ofMillis(500))).log();
    }

    public static Flux<Integer> shareFlux() {
        return coldFluxDefer()
            .share()
            .log();
    }

    public static Flux<Integer> connectFlux() {
        return coldFluxDefer()
            .publish()
            .autoConnect(1)
            .log();
    }

    public static Flux<Integer> replayFlux() {
        return coldFluxDefer()
            .replay()
            .autoConnect(1)
            .log();
    }

}
