package com.learnreactiveprogramming.service;

import com.learnreactiveprogramming.exception.ReactorException;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

@Slf4j
public class FluxAndMonoGeneratorService {

    public static void main(String[] args) {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        fluxAndMonoGeneratorService.namesFluxFilter(4)
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
            .publishOn(Schedulers.parallel())
            .filter(name -> {
                log.info(String.format("Filtering for name %s", name));
                return name.length() > stringLength;
            })
            .map(name -> name.length() + " - " + name)
            .subscribeOn(Schedulers.boundedElastic())
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
            .doOnNext(name -> System.out.println("name is : " + name))
            .doOnSubscribe(sub -> System.out.println("Subscription is : " + sub))
            .doFinally(s -> System.out.println("Finally is : " + s))
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

    public Flux<String> exploreConcat() {
        Flux<String> abcFlux = Flux.just("A", "B", "C");
        Flux<String> defFlux = Flux.just("D", "E", "F");
        return Flux.concat(abcFlux, defFlux).log();
    }

    public Flux<String> exploreConcatWith() {
        Flux<String> abcFlux = Flux.just("A", "B", "C");
        Flux<String> defFlux = Flux.just("D", "E", "F");
        return abcFlux.concatWith(defFlux).log();
    }


    public Flux<String> exploreConcatMono() {
        var aMono = Mono.just("A");
        var bMono = Mono.just("B");
        return Flux.concat(aMono, bMono).log();
    }

    public Flux<String> exploreConcatWithMono() {
        var aMono = Mono.just("A");
        var bMono = Mono.just("B");
        return aMono.concatWith(bMono).log();
    }


    public Flux<String> exploreMerge() {
        Flux<String> abcFlux = Flux.just("A", "B", "C")
            .delayElements(Duration.ofMillis(100));
        Flux<String> defFlux = Flux.just("D", "E", "F")
            .delayElements(Duration.ofMillis(100));
        return Flux.merge(abcFlux, defFlux).log();
    }

    public Flux<String> exploreMergeWith() {
        Flux<String> abcFlux = Flux.just("A", "B", "C")
            .delayElements(Duration.ofMillis(100));
        Flux<String> defFlux = Flux.just("D", "E", "F")
            .delayElements(Duration.ofMillis(100));
        return abcFlux.mergeWith(defFlux).log();
    }

    public Flux<String> exploreMergeMono() {
        var aMono = Mono.just("A");
        var bMono = Flux.just("B");
        return Flux.merge(aMono, bMono).log();
    }

    public Flux<String> exploreMergeWithMono() {
        var aMono = Mono.just("A");
        var bMono = Flux.just("B");
        return aMono.mergeWith(bMono).log();
    }

    public Flux<String> exploreMergeSequential() {
        Flux<String> abcFlux = Flux.just("A", "B", "C");
        Flux<String> defFlux = Flux.just("D", "E", "F");
        return Flux.mergeSequential(abcFlux, defFlux).log();
    }

    public Flux<String> exploreZip() {
        Flux<String> abcFlux = Flux.just("A", "B", "C")
            .delayElements(Duration.ofMillis(100));
        Flux<String> defFlux = Flux.just("D", "E", "F")
            .delayElements(Duration.ofMillis(100));
        Flux<String> flux123 = Flux.just("1", "2", "3")
            .delayElements(Duration.ofMillis(100));
        Flux<String> flux456 = Flux.just("4", "5", "6")
            .delayElements(Duration.ofMillis(100));
        return Flux.zip(abcFlux, defFlux, flux123, flux456)
            .map(t -> t.getT1() + t.getT2() + t.getT3() +  t.getT4());
    }

    public Flux<String> exploreZipWith() {
        Flux<String> abcFlux = Flux.just("A", "B", "C")
            .delayElements(Duration.ofMillis(100));
        Flux<String> defFlux = Flux.just("D", "E", "F")
            .delayElements(Duration.ofMillis(100));
        return abcFlux.zipWith(defFlux)
            .map(t -> t.getT1() + t.getT2());
    }

    public Flux<String> exceptionFlux() {
        return Flux.just("A", "B", "C")
            .concatWith(Flux.error(new RuntimeException("error")))
            .concatWith(Flux.just("D"));
    }

    public Flux<String> exploreOnErrorReturn() {
        return Flux.just("A", "B", "C")
            .concatWith(Flux.error(new IllegalStateException("Exception Occurred")))
            .onErrorReturn("D");
    }

    public Flux<String> exploreOnErrorResume(Exception e) {
        return Flux.just("A", "B", "C")
            .concatWith(Flux.error(e))
            .onErrorResume(ex -> {
                log.error("Exception is ", ex);
                return Flux.just("D", "E", "F");
            });
    }

    public Flux<String> exploreOnErrorContinue() {
        return Flux.just("A", "B", "C")
            .map(s -> {
                if (s.equals("B")) {
                    throw new IllegalStateException("Exception Occurred");
                }
                return s;
            })
            .onErrorContinue((ex, value) -> {
                log.error("Value is {}, Exception is ", value, ex);
            });
    }

    public Flux<String> exploreOnErrorMap() {
        return Flux.just("A", "B", "C")
            .map(s -> {
                if (s.equals("B")) {
                    throw new IllegalStateException("Exception Occurred");
                }
                return s;
            })
            .onErrorMap(ex -> new ReactorException(ex, ex.getMessage()))
            .log();
    }

    public Flux<String> exploreDoOnError() {
        return Flux.just("A", "B", "C")
            .map(s -> {
                if (s.equals("B")) {
                    throw new IllegalStateException("Exception Occurred");
                }
                return s;
            })
            .doOnError(ex -> log.error("Exception is ", ex))
            .log();
    }

    private static Flux<String> splitWithDelay(String name) {
        int delay = new Random().nextInt(1000);
        return Flux.fromArray(name.split("")).delayElements(Duration.ofMillis(delay));
    }

}
