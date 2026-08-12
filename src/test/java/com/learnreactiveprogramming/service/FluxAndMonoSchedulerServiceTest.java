package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.*;

class FluxAndMonoSchedulerServiceTest {

    private FluxAndMonoSchedulerService fluxAndMonoSchedulerService = new FluxAndMonoSchedulerService();

    @Test
    void explorePublishOn() {
        Flux<String> flux = fluxAndMonoSchedulerService.explorePublishOn();
        StepVerifier.create(flux)
            .expectNextCount(6)
            .verifyComplete();
    }


    @Test
    void exploreSubscribeOn() {
        Flux<String> flux = fluxAndMonoSchedulerService.exploreSubscribeOn();
        StepVerifier.create(flux)
            .expectNextCount(6)
            .verifyComplete();
    }
}