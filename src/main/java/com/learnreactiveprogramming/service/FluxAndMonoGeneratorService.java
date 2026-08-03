package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public class FluxAndMonoGeneratorService {

    public static void main(String[] args) {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        fluxAndMonoGeneratorService.namesFlux()
            .subscribe(name -> System.out.println("name is : " + name));
        fluxAndMonoGeneratorService.nameMono()
            .subscribe(name -> System.out.println("Mono name is : " + name));
    }

    public Flux<String> namesFlux() {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
            .log();
    }


    public Mono<String> nameMono() {
        return Mono.just("alex");
    }

}
