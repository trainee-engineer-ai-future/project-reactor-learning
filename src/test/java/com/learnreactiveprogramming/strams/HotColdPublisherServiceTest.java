package com.learnreactiveprogramming.strams;

import com.learnreactiveprogramming.streams.HotColdPublisherService;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class HotColdPublisherServiceTest {


    @Test
    public void coldFluxDefer() {
        Flux<Integer> integerFlux = HotColdPublisherService.coldFluxDefer();
        StepVerifier.create(integerFlux)
            .expectNext(1, 2, 3)
            .verifyComplete();

        StepVerifier.create(integerFlux)
            .expectNext(1, 2, 3)
            .verifyComplete();
    }

}
