package com.learnreactiveprogramming.service;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static com.learnreactiveprogramming.util.CommonUtil.delay;
@Slf4j
public class FluxAndMonoSchedulerService {

    static List<String> namesList = List.of("alex", "ben", "chloe");
    static List<String> namesList1 = List.of("adam", "jill", "jack");

    public Flux<String> explorePublishOn() {
        var nameFlux = Flux.fromIterable(namesList)
            .publishOn(Schedulers.parallel())
            .map(this::upperCase)
            .log();

        var nameFlux1 = Flux.fromIterable(namesList1)
            .publishOn(Schedulers.parallel())
            .map(this::upperCase)
            .log();

        return nameFlux.mergeWith(nameFlux1);
    }

    public Flux<String> exploreSubscribeOn() {
        var nameFlux = flux(namesList)
            .subscribeOn(Schedulers.boundedElastic())
            .publishOn(Schedulers.parallel())
            .map(s -> {
                log.debug("s : {}", s);
                return s;
            });

        var nameFlux1 = flux(namesList1)
            .subscribeOn(Schedulers.boundedElastic());

        return nameFlux.mergeWith(nameFlux1);
    }

    private Flux<String> flux(List<String> names) {
        return Flux.fromIterable(namesList)
            .map(this::upperCase)
            .log();
    }

    private String upperCase(String str) {
        delay(1000);
        return str.toUpperCase();
    }
}
