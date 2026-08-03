package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.List;

public class FluxAndMonoGeneratorServiceTest {

    FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();

    @Test
    public void namesFlux() {
        //given
        //when
        var namesFlux = fluxAndMonoGeneratorService.namesFlux();
        //then
        StepVerifier.create(namesFlux)
//            .expectNext("alex", "ben", "chloe")
//            .expectNextCount(3)
            .expectNext("alex")
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    public void nameMono() {
        //given
        //when
        var nameMono = fluxAndMonoGeneratorService.nameMono();
        //then
        StepVerifier.create(nameMono)
            .expectNext("alex")
            .expectNextCount(0)
            .verifyComplete();
    }

    @Test
    void namesFluxMap() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.namesFluxMap();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("ALEX", "BEN", "CHLOE")
            .verifyComplete();
    }

    @Test
    void namesFluxImmutability() {
        //given
        //when
        var namesFluxImmutability = fluxAndMonoGeneratorService.namesFluxImmutability();
        //then
        StepVerifier.create(namesFluxImmutability)
            .expectNext("alex", "ben", "chloe")
            .verifyComplete();
    }

    @Test
    void namesFluxFilter() {
        //given
        int nameLength = 3;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.namesFluxFilter(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("4 - ALEX", "5 - CHLOE")
            .verifyComplete();
    }

    @Test
    void nameMonoFilter() {
        //given
        int nameLength = 3;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.nameMonoFilter(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("4 - ALEX")
            .verifyComplete();
    }

    @Test
    void nameMonoFilterDefaultIfEmpty() {
        //given
        int nameLength = 6;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.nameMonoFilterDefaultIfEmpty(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("7 - DEFAULT")
            .verifyComplete();
    }

    @Test
    void nameMonoFilterSwitchIfEmpty() {
        //given
        int nameLength = 6;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.nameMonoFilterSwitchIfEmpty(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("7 - DEFAULT")
            .verifyComplete();
    }

    @Test
    void namesFluxFlatMap() {
        //given
        int nameLength = 3;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.namesFluxFlatMap(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "L", "E", "X", "C", "H", "L", "O", "E")
            .verifyComplete();
    }

    @Test
    void namesFluxFlatMapAsync() {
        //given
        int nameLength = 3;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.namesFluxFlatMapAsync(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNextCount(9)
            .verifyComplete();
    }

    @Test
    void namesFluxConcatMapAsync() {
        //given
        int nameLength = 3;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.namesFluxConcatMapAsync(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "L", "E", "X", "C", "H", "L", "O", "E")
            .verifyComplete();
    }

    @Test
    void nameMonoFlatMap() {
        //given
        int nameLength = 3;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.nameMonoFlatMap(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext(List.of("a", "l", "e", "x"))
            .verifyComplete();
    }

    @Test
    void nameMonoFlatMapMany() {
        //given
        int nameLength = 3;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.nameMonoFlatMapMany(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("a", "l", "e", "x")
            .verifyComplete();
    }

    @Test
    void namesFluxTransform() {
        //given
        int nameLength = 3;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.namesFluxTransform(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "L", "E", "X", "C", "H", "L", "O", "E")
            .verifyComplete();
    }

    @Test
    void namesFluxTransformOne() {
        //given
        int nameLength = 6;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.namesFluxTransform(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("default")
            .verifyComplete();
    }

    @Test
    void namesFluxTransformTwo() {
        //given
        int nameLength = 6;
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.namesFluxTransformSwitchIfEmpty(nameLength);
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("D", "E", "F", "A", "U", "L", "T")
            .verifyComplete();
    }
}
