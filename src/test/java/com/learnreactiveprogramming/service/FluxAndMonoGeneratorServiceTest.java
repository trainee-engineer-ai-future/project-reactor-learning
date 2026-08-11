package com.learnreactiveprogramming.service;

import com.learnreactiveprogramming.exception.ReactorException;
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

    @Test
    void exploreConcat() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.exploreConcat();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "B", "C", "D", "E", "F")
            .verifyComplete();
    }

    @Test
    void exploreConcatWith() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.exploreConcatWith();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "B", "C", "D", "E", "F")
            .verifyComplete();
    }

    @Test
    void exploreConcatMono() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.exploreConcatMono();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "B")
            .verifyComplete();
    }

    @Test
    void exploreConcatWithMono() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.exploreConcatWithMono();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "B")
            .verifyComplete();
    }

    @Test
    void exploreMerge() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.exploreMerge();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "D","B", "E", "C", "F")
            .verifyComplete();
    }

    @Test
    void exploreMergeWith() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.exploreMergeWith();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "D","B", "E", "C", "F")
            .verifyComplete();
    }

    @Test
    void exploreMergeMono() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.exploreMergeMono();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "B")
            .verifyComplete();
    }

    @Test
    void exploreMergeWithMono() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.exploreMergeWithMono();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "B")
            .verifyComplete();
    }

    @Test
    void exploreMergeSequential() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.exploreMergeSequential();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("A", "B", "C", "D", "E", "F")
            .verifyComplete();
    }

    @Test
    void exploreZip() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.exploreZip();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("AD14", "BE25", "CF36")
            .verifyComplete();
    }

    @Test
    void exploreZipWith() {
        //given
        //when
        var namesFluxMap = fluxAndMonoGeneratorService.exploreZipWith();
        //then
        StepVerifier.create(namesFluxMap)
            .expectNext("AD", "BE", "CF")
            .verifyComplete();
    }

    @Test
    void exceptionFlux() {
        //given
        //when
        var exceptionFlux = fluxAndMonoGeneratorService.exceptionFlux();
        //then
        StepVerifier.create(exceptionFlux)
            .expectNext("A", "B", "C")
            .verifyError(RuntimeException.class);
    }

    @Test
    void exploreOnErrorReturn() {
        //given
        //when
        var exceptionFlux = fluxAndMonoGeneratorService.exploreOnErrorReturn();
        //then
        StepVerifier.create(exceptionFlux)
            .expectNext("A", "B", "C", "D")
            .verifyComplete();
    }

    @Test
    void exploreOnErrorResume() {
        //given
        //when
        var exceptionFlux = fluxAndMonoGeneratorService.exploreOnErrorResume(new RuntimeException("test"));
        //then
        StepVerifier.create(exceptionFlux)
            .expectNext("A", "B", "C", "D", "E", "F")
            .verifyComplete();
    }

    @Test
    void exploreOnErrorContinue() {
        //given
        //when
        var exceptionFlux = fluxAndMonoGeneratorService.exploreOnErrorContinue();
        //then
        StepVerifier.create(exceptionFlux)
            .expectNext("A", "C")
            .verifyComplete();
    }

    @Test
    void exploreOnErrorMap() {
        //given
        //when
        var exceptionFlux = fluxAndMonoGeneratorService.exploreOnErrorMap();
        //then
        StepVerifier.create(exceptionFlux)
            .expectNext("A")
            .verifyError(ReactorException.class);
    }

    @Test
    void exploreDoOnError() {
        //given
        //when
        var exceptionFlux = fluxAndMonoGeneratorService.exploreDoOnError();
        //then
        StepVerifier.create(exceptionFlux)
            .expectNext("A")
            .verifyError(RuntimeException.class);
    }
}
