package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class FluxAndMonoGeneratorService {

    public static void main(String[] args) {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        fluxAndMonoGeneratorService.namesFluxMap()
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

    public Mono<String> nameMonoFilter(int stringLength) {
        return Mono.just("alex")
            .filter(s -> s.length() > stringLength)
            .map(name -> name.length() + " - " + name.toUpperCase());
    }

    public Mono<String> nameMonoFilterDefaultIfEmpty(int stringLength) {
        return Mono.just("alex")
            .filter(s -> s.length() > stringLength)
            .defaultIfEmpty("default")
            .map(name -> name.length() + " - " + name.toUpperCase());
    }

    public Mono<String> nameMonoFilterSwitchIfEmpty(int stringLength) {
        return Mono.just("alex")
            .filter(s -> s.length() > stringLength)
            .switchIfEmpty(Mono.just("default"))
            .map(name -> name.length() + " - " + name.toUpperCase());
    }

    public Mono<List<String>> nameMonoFlatMap(int stringLength) {
        return Mono.just("alex")
            .filter(s -> s.length() > stringLength)
            .flatMap(name -> Mono.just(List.of(name.split(""))));
    }

    public Flux<String> nameMonoFlatMapMany(int stringLength) {
        return Mono.just("alex")
            .filter(s -> s.length() > stringLength)
            .flatMapMany(FluxAndMonoGeneratorService::splitWithDelay);
    }

    public Flux<String> namesFluxMap() {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
            .map(String::toUpperCase)
            .log();
    }

    public Flux<String> namesFluxImmutability() {
        var namesFlux = Flux.fromIterable(List.of("alex", "ben", "chloe"));
        namesFlux.map(String::toUpperCase)
            .log();
        return namesFlux;
    }

    public Flux<String> namesFluxFilter(int stringLength) {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
            .map(String::toUpperCase)
            .filter(name -> name.length() > stringLength)
            .map(name -> name.length() + " - " + name)
            .log();
    }

    public Flux<String> namesFluxFlatMap(int stringLength) {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
            .map(String::toUpperCase)
            .filter(name -> name.length() > stringLength)
            .flatMap(name -> Flux.fromArray(name.split("")))
            .log();
    }

    public Flux<String> namesFluxFlatMapAsync(int stringLength) {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
            .map(String::toUpperCase)
            .filter(name -> name.length() > stringLength)
            .flatMap(FluxAndMonoGeneratorService::splitWithDelay)
            .log();
    }

    public Flux<String> namesFluxConcatMapAsync(int stringLength) {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
            .map(String::toUpperCase)
            .filter(name -> name.length() > stringLength)
            .concatMap(FluxAndMonoGeneratorService::splitWithDelay)
            .log();
    }

    public Flux<String> namesFluxTransform(int stringLength) {
        Function<Flux<String>, Flux<String>> filterMap = nameFlux ->
            nameFlux
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(name -> Flux.fromArray(name.split("")));
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
            .transform(filterMap)
            .defaultIfEmpty("default")
            .log();
    }

    public Flux<String> namesFluxTransformSwitchIfEmpty(int stringLength) {
        Function<Flux<String>, Flux<String>> filterMap = nameFlux ->
            nameFlux
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(name -> Flux.fromArray(name.split("")));
        Flux<String> defaultF = Flux.just("default").transform(filterMap);
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
            .transform(filterMap)
            .switchIfEmpty(defaultF)
            .log();
    }

    private static Flux<String> splitWithDelay(String name) {
        int delay = new Random().nextInt(1000);
        return Flux.fromArray(name.split("")).delayElements(Duration.ofMillis(delay));
    }

}
